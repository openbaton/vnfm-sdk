/*
 * Copyright (c) 2016 Open Baton (http://www.openbaton.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.openbaton.common.vnfm_sdk.amqp;

import com.google.gson.Gson;
import com.rabbitmq.client.*;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;
import org.openbaton.catalogue.nfvo.messages.Interfaces.NFVMessage;
import org.openbaton.common.vnfm_sdk.AbstractVnfm;
import org.openbaton.common.vnfm_sdk.VnfmHelper;
import org.openbaton.common.vnfm_sdk.exception.BadFormatException;
import org.openbaton.common.vnfm_sdk.exception.NotFoundException;
import org.openbaton.registration.Registration;
import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/** Created by lto on 28/05/15. */
@SpringBootApplication
@ComponentScan(basePackages = "org.openbaton")
@ConfigurationProperties
public abstract class AbstractVnfmSpringAmqp extends AbstractVnfm {

  @Value("${spring.rabbitmq.host}")
  private String rabbitHost;

  @Value("${spring.rabbitmq.port}")
  private int rabbitPort;

  @Value("${spring.rabbitmq.username}")
  private String rabbitUsername;

  @Value("${spring.rabbitmq.password}")
  private String rabbitPassword;

  @Value("${spring.rabbitmq.virtualHost:/}")
  private String virtualHost;

  @Value("${vnfm.consumers.num:5}")
  private int consumers;

  @Value("${vnfm.connect.tries:20}")
  private int maxTries;

  @Value("${vnfm.connect.tries.retryPause:2500}")
  private int retryPauseTries;

  @Value("${vnfm.connect.tries.authentication:3}")
  private int maxAuthenticationTries;

  @Value("${vnfm.connect.tries.authentication.retryPause:40000}")
  private int baseRetryPauseAuthenticationTries;

  @Autowired
  @Qualifier("vnfmGson")
  private Gson gson;

  @Autowired private ConfigurableApplicationContext context;
  @Autowired private Registration registration;

  @Override
  protected void setup() {
    vnfmHelper = (VnfmHelper) context.getBean("vnfmSpringHelperRabbit");
    super.setup();
  }

  private class ConsumerRunnable implements Runnable {
    @Override
    public void run() {
      ConnectionFactory connectionFactory = new ConnectionFactory();
      connectionFactory.setHost(rabbitHost);
      connectionFactory.setPort(rabbitPort);
      connectionFactory.setUsername(rabbitUsername);
      connectionFactory.setPassword(rabbitPassword);
      connectionFactory.setVirtualHost(virtualHost);
      try (Connection connection = connectionFactory.newConnection()) {
        final Channel channel = connection.createChannel();
        channel.basicQos(1);
        DefaultConsumer consumer =
            new DefaultConsumer(channel) {

              @Override
              public void handleDelivery(
                  String consumerTag,
                  Envelope envelope,
                  AMQP.BasicProperties properties,
                  byte[] body)
                  throws IOException {
                AMQP.BasicProperties replyProps =
                    new AMQP.BasicProperties.Builder()
                        .correlationId(properties.getCorrelationId())
                        .contentType("plain/text")
                        .build();

                NFVMessage answerMessage = null;
                try {
                  NFVMessage nfvMessage =
                      gson.fromJson(
                          getStringFromInputStream(new ByteArrayInputStream(body)),
                          NFVMessage.class);

                  answerMessage = onAction(nfvMessage);
                } catch (NotFoundException | BadFormatException e) {
                  log.error("Error while processing message from NFVO");
                  e.printStackTrace();
                } finally {
                  String answer = gson.toJson(answerMessage);
                  channel.basicPublish(
                      "", properties.getReplyTo(), replyProps, answer.getBytes("UTF-8"));
                  channel.basicAck(envelope.getDeliveryTag(), false);
                }
              }
            };
        channel.basicConsume(((VnfmSpringHelperRabbit) vnfmHelper).getVnfmEndpoint(), false, consumer);

        //loop to prevent reaching finally block
        while (true) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            log.info("Ctrl-c received");
            System.exit(0);
          }
        }
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    }
  }

  private void listenOnQueues() {
    log.debug("Start listening on queues");

    for (int i = 0; i < consumers; i++) {
      Runnable listenerRunnable = new ConsumerRunnable();
      Thread thread = new Thread(listenerRunnable);
      thread.setDaemon(true);
      thread.start();
    }
    log.info("Started " + consumers + " consumers");
  }

  /**
   * Deregisters the VNFM from the NFVO as soon as Spring sends its ContextClosedEvent.
   *
   * @param event the Spring ContextClosedEvent
   */
  @EventListener
  protected void unregister(ContextClosedEvent event) {
    unregister();
  }

  @Override
  protected void unregister() {
    try {
      if (!registration.hasUsername()) {
        log.trace("VNFM did not register yet, so no deregistration necessary.");
        return;
      }
      registration.deregisterVnfmFromNfvo(
          ((VnfmSpringHelperRabbit) vnfmHelper).getRabbitTemplate(), vnfmManagerEndpoint);
      ((VnfmSpringHelperRabbit) vnfmHelper)
          .deleteQueue(
              ((VnfmSpringHelperRabbit) vnfmHelper).getVnfmEndpoint(),
              rabbitHost,
              rabbitPort,
              rabbitUsername,
              rabbitPassword);
    } catch (IllegalStateException | TimeoutException | IOException e) {
      log.error("Got exception while deregistering the VNFM from the NFVO");
    }
  }

  /**
   * Registers the VNFM to the NFVO as soon as Spring sends its ContextRefreshedEvent.
   *
   * @param event the Spring ContextRefreshedEvent
   */
  @EventListener
  private void register(ContextRefreshedEvent event) {
    register();
  }

  @Override
  protected void register() {
    String[] usernamePassword;

    final boolean[] tryToRegister = {true};

    Thread shutdownHook =
        new Thread(() -> tryToRegister[0] = false);
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    int authenticationTries = 0;
    int tries = 0;
    if (maxTries < 0) maxTries = Integer.MAX_VALUE;
    while (true) {
      if (!tryToRegister[0]) {
        context.close();
        return;
      }
      try {
        usernamePassword =
            registration.registerVnfmToNfvo(
                ((VnfmSpringHelperRabbit) vnfmHelper).getRabbitTemplate(), vnfmManagerEndpoint);
        break;
      } catch (AmqpAuthenticationException | IllegalArgumentException e) {
        int retryPause = retryPauseTries;
        if (e instanceof AmqpAuthenticationException) {
          retryPause = baseRetryPauseAuthenticationTries * ((int) Math.pow(2, authenticationTries));
          authenticationTries++;
          log.debug(
              "VNFM registration not successful. Waiting in case the NFVO has not created the RabbitMQ "
                  + "'openbaton-manager-user' user yet: "
                  + (maxAuthenticationTries - authenticationTries)
                  + " attempt(s) left");
        } else {
          log.debug("Registration failed: " + e.getMessage());
          tries++;
        }
        if ((tries >= (maxTries)) || (authenticationTries >= (maxAuthenticationTries))) {
          e.printStackTrace();
          System.exit(1);
        }
        try {
          log.debug("Try again in " + retryPause / 1000 + " seconds.");
          Thread.sleep(retryPause);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
    Runtime.getRuntime().removeShutdownHook(shutdownHook);

    this.rabbitUsername = usernamePassword[0];
    this.rabbitPassword = usernamePassword[1];

    try {
      ((VnfmSpringHelperRabbit) vnfmHelper)
          .createQueue(
              rabbitHost,
              rabbitPort,
              rabbitUsername,
              rabbitPassword,
              virtualHost,
              vnfmHelper.getVnfmEndpoint(),
              "openbaton-exchange");
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
        unregister();
    System.exit(34);
    }

    log.info("Correctly registered to NFVO");
    listenOnQueues();
  }

  private static String getStringFromInputStream(InputStream is) {

    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();

    String line;
    try {

      br = new BufferedReader(new InputStreamReader(is));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return sb.toString();
  }
}
