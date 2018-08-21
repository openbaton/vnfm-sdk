/*
 * Copyright (c) 2015-2018 Open Baton (http://openbaton.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openbaton.common.vnfm_sdk.amqp;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeoutException;
import javax.annotation.PostConstruct;
import org.openbaton.catalogue.nfvo.EndpointType;
import org.openbaton.catalogue.nfvo.messages.Interfaces.NFVMessage;
import org.openbaton.common.vnfm_sdk.VnfmHelper;
import org.openbaton.common.vnfm_sdk.amqp.configuration.RabbitConfiguration;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope
@ConfigurationProperties
public class VnfmSpringHelperRabbit extends VnfmHelper {

  @Autowired
  @Qualifier("vnfmGson")
  private Gson gson;

  @Value("${vnfm.rabbitmq.autodelete}")
  private boolean autodelete = true;

  @Value("${vnfm.rabbitmq.durable}")
  private boolean durable;

  @Value("${vnfm.rabbitmq.exclusive}")
  private boolean exclusive;

  @Value("${vnfm.type:unknown}")
  private String vnfmType;

  @Value("${vnfm.enabled:true}")
  private boolean enabled;

  @Override
  public String getVnfmDescription() {
    return vnfmDescription;
  }

  @Override
  public boolean isVnfmEnabled() {
    return this.enabled;
  }

  public void setVnfmDescription(String vnfmDescription) {
    this.vnfmDescription = vnfmDescription;
  }

  @Value("${vnfm.description:unknown}")
  private String vnfmDescription;

  @Value("${vnfm.endpoint:unknown}")
  private String vnfmEndpoint;

  @Value("${vnfm.endpoint.type:RABBIT}")
  private EndpointType vnfmEndpointType;

  @Value("${vnfm.rabbitmq.virtual-host:/}")
  private String virtualHost;

  public Gson getGson() {
    return gson;
  }

  public void setGson(Gson gson) {
    this.gson = gson;
  }

  public String getVirtualHost() {
    return virtualHost;
  }

  public void setVirtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
  }

  public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
    this.rabbitTemplate = rabbitTemplate;
  }

  @Autowired private RabbitTemplate rabbitTemplate;

  @Value("${vnfm.rabbitmq.sar.timeout:1000}")
  private int timeout;

  public boolean isExclusive() {
    return exclusive;
  }

  public void setExclusive(boolean exclusive) {
    this.exclusive = exclusive;
  }

  public boolean isDurable() {
    return durable;
  }

  public void setDurable(boolean durable) {
    this.durable = durable;
  }

  public boolean isAutodelete() {
    return autodelete;
  }

  public void setAutodelete(boolean autodelete) {
    this.autodelete = autodelete;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public RabbitTemplate getRabbitTemplate() {
    return rabbitTemplate;
  }

  @PostConstruct
  private void init() {
    this.rabbitTemplate.setExchange("openbaton-exchange"); //TODO
  }

  public void sendMessageToQueue(String sendToQueueName, final Serializable message) {
    log.debug("Sending message to Queue:  " + sendToQueueName);
    rabbitTemplate.convertAndSend(sendToQueueName, gson.toJson(message));
  }

  @Override
  public String getVnfmType() {
    return this.vnfmType;
  }

  @Override
  public void setVnfmType(String vnfmType) {
    this.vnfmType = vnfmType;
  }

  @Override
  public String getVnfmEndpoint() {
    return this.vnfmEndpoint;
  }

  @Override
  public void setVnfmEndpoint(String vnfmEndpoint) {
    this.vnfmEndpoint = vnfmEndpoint;
  }

  @Override
  public EndpointType getVnfmEndpointType() {
    return this.vnfmEndpointType;
  }

  @Override
  public void setVnfmEndpointType(EndpointType vnfmEndpointType) {
    this.vnfmEndpointType = vnfmEndpointType;
  }

  @Override
  public void sendToNfvo(final NFVMessage nfvMessage) {
    sendMessageToQueue(RabbitConfiguration.queueName_vnfmCoreActions, nfvMessage);
  }

  @Override
  public NFVMessage sendAndReceive(NFVMessage message) throws Exception {

    rabbitTemplate.setReplyTimeout(timeout * 1000);
    rabbitTemplate.afterPropertiesSet();
    String response =
        (String)
            this.rabbitTemplate.convertSendAndReceive(
                RabbitConfiguration.queueName_vnfmCoreActionsReply, gson.toJson(message));

    return gson.fromJson(response, NFVMessage.class);
  }

  @Override
  public String sendAndReceive(String message, String queueName) throws Exception {

    rabbitTemplate.setReplyTimeout(timeout * 1000);
    rabbitTemplate.afterPropertiesSet();

    log.debug("Sending to: " + queueName);
    String res =
        (String) rabbitTemplate.convertSendAndReceive("openbaton-exchange", queueName, message);
    log.trace("Received from EMS: " + res);
    if (res == null) {
      log.error("After " + timeout + " seconds the ems did not answer.");
      throw new TimeoutException(
          "After "
              + timeout
              + " seconds the ems did not answer. You can change this value by editing the application.properties propery \"vnfm.rabbitmq.sar.timeout\"");
    }
    return res;
  }

  public void createQueue(
      String brokerIp,
      int port,
      String rabbitUsername,
      String rabbitPassword,
      String virtualHost,
      String queue,
      String exchange)
      throws IOException, TimeoutException {
    ConnectionFactory factory =
        getConnectionFactory(brokerIp, port, rabbitUsername, rabbitPassword, virtualHost);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(queue, durable, exclusive, autodelete, null);
    channel.queueBind(queue, exchange, queue);
    channel.basicQos(1);
    channel.close();
    connection.close();
  }

  private ConnectionFactory getConnectionFactory(
      String brokerIp, int port, String rabbitUsername, String rabbitPassword, String virtualHost) {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(brokerIp);
    factory.setPort(port);
    factory.setUsername(rabbitUsername);
    factory.setPassword(rabbitPassword);
    factory.setVirtualHost(virtualHost);
    return factory;
  }

  public void deleteQueue(
      String queueName, String brokerIp, int port, String rabbitUsername, String rabbitPassword)
      throws IOException, TimeoutException {
    ConnectionFactory factory =
        getConnectionFactory(brokerIp, port, rabbitUsername, rabbitPassword, virtualHost);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    //    channel.exchangeDeclare(exchange, "topic", true);
    channel.queueDelete(queueName, false, false);
  }
}
