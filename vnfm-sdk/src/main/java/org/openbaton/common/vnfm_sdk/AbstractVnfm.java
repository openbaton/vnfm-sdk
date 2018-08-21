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

package org.openbaton.common.vnfm_sdk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.openbaton.catalogue.mano.descriptor.InternalVirtualLink;
import org.openbaton.catalogue.mano.descriptor.VNFComponent;
import org.openbaton.catalogue.mano.descriptor.VirtualDeploymentUnit;
import org.openbaton.catalogue.mano.descriptor.VirtualNetworkFunctionDescriptor;
import org.openbaton.catalogue.mano.record.VNFCInstance;
import org.openbaton.catalogue.mano.record.VNFRecordDependency;
import org.openbaton.catalogue.mano.record.VirtualLinkRecord;
import org.openbaton.catalogue.mano.record.VirtualNetworkFunctionRecord;
import org.openbaton.catalogue.nfvo.Action;
import org.openbaton.catalogue.nfvo.Script;
import org.openbaton.catalogue.nfvo.VnfmManagerEndpoint;
import org.openbaton.catalogue.nfvo.messages.*;
import org.openbaton.catalogue.nfvo.messages.Interfaces.NFVMessage;
import org.openbaton.catalogue.nfvo.viminstances.BaseVimInstance;
import org.openbaton.catalogue.security.Key;
import org.openbaton.common.vnfm_sdk.exception.BadFormatException;
import org.openbaton.common.vnfm_sdk.exception.NotFoundException;
import org.openbaton.common.vnfm_sdk.exception.VnfmSdkException;
import org.openbaton.common.vnfm_sdk.interfaces.LogDispatcher;
import org.openbaton.common.vnfm_sdk.interfaces.VNFLifecycleChangeNotification;
import org.openbaton.common.vnfm_sdk.interfaces.VNFLifecycleManagement;
import org.openbaton.common.vnfm_sdk.utils.VNFRUtils;
import org.openbaton.common.vnfm_sdk.utils.VnfmUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by lto on 08/07/15. */
public abstract class AbstractVnfm
    implements VNFLifecycleManagement, VNFLifecycleChangeNotification {

  protected static String type;
  protected static String endpoint;
  protected static String endpointType;
  protected static Properties properties;
  protected static Logger log = LoggerFactory.getLogger(AbstractVnfm.class);
  protected VnfmHelper vnfmHelper;
  protected VnfmManagerEndpoint vnfmManagerEndpoint;
  private ExecutorService executor;
  protected static String brokerIp;
  protected static String brokerPort;
  protected static String monitoringIp;
  protected static String timezone;
  protected static String username;
  protected static String password;
  protected static String exchangeName;
  protected static String nsrId;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  private boolean enabled;

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  private String description;

  // If the VNFM supports the requesting of log files, it can instantiate this field (e.g. in the setup method), otherwise it will not be used.
  protected LogDispatcher logDispatcher;

  @PreDestroy
  private void shutdown() {}

  @PostConstruct
  private void init() {
    setup();
    executor =
        Executors.newFixedThreadPool(Integer.parseInt(properties.getProperty("concurrency", "15")));
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @Override
  public abstract void query();

  @Override
  public abstract VirtualNetworkFunctionRecord scale(
      Action scaleInOrOut,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord,
      VNFComponent component,
      Object scripts,
      VNFRecordDependency dependency)
      throws Exception;

  @Override
  public abstract void checkInstantiationFeasibility();

  @Override
  public abstract VirtualNetworkFunctionRecord heal(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord,
      VNFCInstance component,
      String cause)
      throws Exception;

  @Override
  public abstract VirtualNetworkFunctionRecord updateSoftware(
      Script script, VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) throws Exception;

  @Override
  public abstract VirtualNetworkFunctionRecord modify(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord, VNFRecordDependency dependency)
      throws Exception;

  @Override
  public abstract void upgradeSoftware();

  @Override
  public abstract VirtualNetworkFunctionRecord terminate(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) throws Exception;

  public abstract void handleError(VirtualNetworkFunctionRecord virtualNetworkFunctionRecord);

  protected void loadProperties() {
    properties = new Properties();
    try {
      properties.load(this.getClass().getClassLoader().getResourceAsStream("conf.properties"));
    } catch (IOException e) {
      e.printStackTrace();
      log.error(e.getLocalizedMessage());
    }
    endpoint = (String) properties.get("endpoint");
    type = (String) properties.get("type");
    endpointType = properties.getProperty("endpoint-type", "RABBIT");
    description = properties.getProperty("description", "");
    enabled = Boolean.parseBoolean(properties.getProperty("enabled", "true"));
  }

  protected NFVMessage onAction(NFVMessage message) throws NotFoundException, BadFormatException {

    VirtualNetworkFunctionRecord virtualNetworkFunctionRecord = null;

    try {
      log.debug(
          "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
              + message.getAction()
              + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      log.trace("VNFM: Received Message: " + message.getAction());
      NFVMessage nfvMessage = null;
      OrVnfmGenericMessage orVnfmGenericMessage = null;
      switch (message.getAction()) {
        case SCALE_IN:
          OrVnfmScalingMessage scalingMessage = (OrVnfmScalingMessage) message;
          nsrId = scalingMessage.getVirtualNetworkFunctionRecord().getParent_ns_id();
          virtualNetworkFunctionRecord = scalingMessage.getVirtualNetworkFunctionRecord();
          VNFCInstance vnfcInstanceToRemove = scalingMessage.getVnfcInstance();

          virtualNetworkFunctionRecord =
              this.scale(
                  Action.SCALE_IN, virtualNetworkFunctionRecord, vnfcInstanceToRemove, null, null);
          nfvMessage = null;
          break;
        case SCALE_OUT:
          scalingMessage = (OrVnfmScalingMessage) message;
          // TODO I don't know if, using a bean of this class the instance can be destroyed and recreated and
          // parameters could be lost
          getExtension(scalingMessage.getExtension());

          nsrId = scalingMessage.getVirtualNetworkFunctionRecord().getParent_ns_id();
          virtualNetworkFunctionRecord = scalingMessage.getVirtualNetworkFunctionRecord();
          VNFRecordDependency dependency = scalingMessage.getDependency();
          VNFComponent component = scalingMessage.getComponent();
          String mode = scalingMessage.getMode();

          log.trace("HB_VERSION == " + virtualNetworkFunctionRecord.getHbVersion());
          log.info("Adding VNFComponent: " + component);
          log.trace("The mode is:" + mode);
          VNFCInstance vnfcInstance_new = null;
          if (!properties.getProperty("allocate", "true").equalsIgnoreCase("true")) {
            NFVMessage message2 =
                vnfmHelper.sendAndReceive(
                    VnfmUtils.getNfvScalingMessage(
                        getUserData(),
                        virtualNetworkFunctionRecord,
                        scalingMessage.getVimInstance()));
            if (message2 instanceof OrVnfmGenericMessage) {
              OrVnfmGenericMessage message1 = (OrVnfmGenericMessage) message2;
              virtualNetworkFunctionRecord = message1.getVnfr();
              log.trace("HB_VERSION == " + virtualNetworkFunctionRecord.getHbVersion());
            } else if (message2 instanceof OrVnfmErrorMessage) {
              this.handleError(((OrVnfmErrorMessage) message2).getVnfr());
              return null;
            }
            vnfcInstance_new = getVnfcInstance(virtualNetworkFunctionRecord, component);
            if (vnfcInstance_new == null) {
              throw new RuntimeException("no new VNFCInstance found. This should not happen...");
            }
            if (mode != null && mode.equalsIgnoreCase("standby")) {
              vnfcInstance_new.setState("STANDBY");
            }
          }

          Object scripts;
          if (scalingMessage.getVnfPackage() == null) {
            scripts = new HashSet<>();
          } else if (scalingMessage.getVnfPackage().getScriptsLink() != null) {
            scripts = scalingMessage.getVnfPackage().getScriptsLink();
          } else {
            scripts = scalingMessage.getVnfPackage().getScripts();
          }

          VirtualNetworkFunctionRecord vnfr =
              this.scale(
                  Action.SCALE_OUT,
                  virtualNetworkFunctionRecord,
                  vnfcInstance_new,
                  scripts,
                  dependency);
          if (vnfcInstance_new == null) {
            log.warn(
                "No new VNFCInstance found, either a bug or was not possible to instantiate it.");
          }
          nfvMessage = VnfmUtils.getNfvMessageScaled(Action.SCALED, vnfr, vnfcInstance_new);
          break;
        case SCALING:
          break;
        case ERROR:
          OrVnfmErrorMessage errorMessage = (OrVnfmErrorMessage) message;
          nsrId = errorMessage.getVnfr().getParent_ns_id();
          log.error("ERROR Received: " + errorMessage.getMessage());
          handleError(errorMessage.getVnfr());

          nfvMessage = null;
          break;
        case MODIFY:
          orVnfmGenericMessage = (OrVnfmGenericMessage) message;
          virtualNetworkFunctionRecord = orVnfmGenericMessage.getVnfr();
          nsrId = orVnfmGenericMessage.getVnfr().getParent_ns_id();
          nfvMessage =
              VnfmUtils.getNfvMessage(
                  Action.MODIFY,
                  this.modify(orVnfmGenericMessage.getVnfr(), orVnfmGenericMessage.getVnfrd()));
          break;
        case RELEASE_RESOURCES:
          orVnfmGenericMessage = (OrVnfmGenericMessage) message;
          nsrId = orVnfmGenericMessage.getVnfr().getParent_ns_id();
          virtualNetworkFunctionRecord = orVnfmGenericMessage.getVnfr();
          nfvMessage =
              VnfmUtils.getNfvMessage(
                  Action.RELEASE_RESOURCES, this.terminate(virtualNetworkFunctionRecord));
          break;
        case INSTANTIATE:
          OrVnfmInstantiateMessage orVnfmInstantiateMessage = (OrVnfmInstantiateMessage) message;
          Map<String, String> extension = orVnfmInstantiateMessage.getExtension();

          log.debug("Extensions are: " + extension);
          log.debug("Keys are: " + orVnfmInstantiateMessage.getKeys());
          getExtension(extension);

          Map<String, Collection<BaseVimInstance>> vimInstances =
              orVnfmInstantiateMessage.getVimInstances();
          if (orVnfmInstantiateMessage.getVnfr() == null) {
            virtualNetworkFunctionRecord =
                createVirtualNetworkFunctionRecord(
                    orVnfmInstantiateMessage.getVnfd(),
                    orVnfmInstantiateMessage.getVnfdf().getFlavour_key(),
                    orVnfmInstantiateMessage.getVlrs(),
                    orVnfmInstantiateMessage.getExtension(),
                    vimInstances);

            log.trace("CREATE: HB VERSION IS: " + virtualNetworkFunctionRecord.getHbVersion());
            GrantOperation grantOperation = new GrantOperation();
            grantOperation.setVirtualNetworkFunctionRecord(virtualNetworkFunctionRecord);

            Future<OrVnfmGrantLifecycleOperationMessage> result = executor.submit(grantOperation);
            OrVnfmGrantLifecycleOperationMessage msg;
            try {
              msg = result.get();
              if (msg == null) {
                return null;
              }
            } catch (ExecutionException e) {
              log.error("Got exception while granting vms");
              throw e.getCause();
            }

            virtualNetworkFunctionRecord = msg.getVirtualNetworkFunctionRecord();
            Map<String, BaseVimInstance> vimInstanceChosen = msg.getVduVim();

            log.trace("GRANT: HB VERSION IS: " + virtualNetworkFunctionRecord.getHbVersion());

            if (!properties.getProperty("allocate", "true").equalsIgnoreCase("true")) {
              AllocateResources allocateResources = new AllocateResources();
              allocateResources.setVirtualNetworkFunctionRecord(virtualNetworkFunctionRecord);
              allocateResources.setVimInstances(vimInstanceChosen);
              allocateResources.setKeyPairs(orVnfmInstantiateMessage.getKeys());
              if (orVnfmInstantiateMessage.getVnfPackage() != null
                  && orVnfmInstantiateMessage.getVnfPackage().getScripts() != null)
                allocateResources.setCustomUserData(
                    getUserDataFromPackage(orVnfmInstantiateMessage.getVnfPackage().getScripts()));
              try {
                virtualNetworkFunctionRecord = executor.submit(allocateResources).get();
                if (virtualNetworkFunctionRecord == null) {
                  return null;
                }
              } catch (ExecutionException e) {
                log.error("Got exception while allocating vms");
                throw e.getCause();
              }
            }
            log.trace("ALLOCATE: HB VERSION IS: " + virtualNetworkFunctionRecord.getHbVersion());
            setupProvides(virtualNetworkFunctionRecord);
          } else virtualNetworkFunctionRecord = orVnfmInstantiateMessage.getVnfr();
          if (orVnfmInstantiateMessage.getVnfPackage() != null) {
            if (orVnfmInstantiateMessage.getVnfPackage().getScriptsLink() != null) {
              virtualNetworkFunctionRecord =
                  instantiate(
                      virtualNetworkFunctionRecord,
                      orVnfmInstantiateMessage.getVnfPackage().getScriptsLink(),
                      vimInstances);
            } else {
              virtualNetworkFunctionRecord =
                  instantiate(
                      virtualNetworkFunctionRecord,
                      orVnfmInstantiateMessage.getVnfPackage().getScripts(),
                      vimInstances);
            }
          } else {
            virtualNetworkFunctionRecord =
                instantiate(virtualNetworkFunctionRecord, null, vimInstances);
          }
          nfvMessage = VnfmUtils.getNfvMessage(Action.INSTANTIATE, virtualNetworkFunctionRecord);
          break;
        case RELEASE_RESOURCES_FINISH:
          break;
        case UPDATE:
          OrVnfmUpdateMessage orVnfmUpdateMessage = (OrVnfmUpdateMessage) message;
          nfvMessage =
              VnfmUtils.getNfvMessage(
                  Action.UPDATE,
                  updateSoftware(orVnfmUpdateMessage.getScript(), orVnfmUpdateMessage.getVnfr()));
          break;
        case HEAL:
          OrVnfmHealVNFRequestMessage orVnfmHealMessage = (OrVnfmHealVNFRequestMessage) message;

          nsrId = orVnfmHealMessage.getVirtualNetworkFunctionRecord().getParent_ns_id();
          VirtualNetworkFunctionRecord vnfrObtained =
              this.heal(
                  orVnfmHealMessage.getVirtualNetworkFunctionRecord(),
                  orVnfmHealMessage.getVnfcInstance(),
                  orVnfmHealMessage.getCause());
          nfvMessage =
              VnfmUtils.getNfvMessageHealed(
                  Action.HEAL, vnfrObtained, orVnfmHealMessage.getVnfcInstance());

          break;
        case INSTANTIATE_FINISH:
          break;
        case CONFIGURE:
          orVnfmGenericMessage = (OrVnfmGenericMessage) message;
          virtualNetworkFunctionRecord = orVnfmGenericMessage.getVnfr();
          nsrId = orVnfmGenericMessage.getVnfr().getParent_ns_id();
          nfvMessage =
              VnfmUtils.getNfvMessage(Action.CONFIGURE, configure(orVnfmGenericMessage.getVnfr()));
          break;
        case START:
          {
            OrVnfmStartStopMessage orVnfmStartStopMessage = (OrVnfmStartStopMessage) message;
            virtualNetworkFunctionRecord = orVnfmStartStopMessage.getVirtualNetworkFunctionRecord();
            nsrId = virtualNetworkFunctionRecord.getParent_ns_id();
            VNFCInstance vnfcInstance = orVnfmStartStopMessage.getVnfcInstance();

            if (vnfcInstance == null) // Start the VNF Record
            {
              nfvMessage =
                  VnfmUtils.getNfvMessage(Action.START, start(virtualNetworkFunctionRecord));
            } else // Start the VNFC Instance
            {
              nfvMessage =
                  VnfmUtils.getNfvMessageStartStop(
                      Action.START,
                      startVNFCInstance(virtualNetworkFunctionRecord, vnfcInstance),
                      vnfcInstance);
            }
            break;
          }
        case STOP:
          {
            OrVnfmStartStopMessage orVnfmStartStopMessage = (OrVnfmStartStopMessage) message;
            virtualNetworkFunctionRecord = orVnfmStartStopMessage.getVirtualNetworkFunctionRecord();
            nsrId = virtualNetworkFunctionRecord.getParent_ns_id();
            VNFCInstance vnfcInstance = orVnfmStartStopMessage.getVnfcInstance();

            if (vnfcInstance == null) // Stop the VNF Record
            {
              nfvMessage = VnfmUtils.getNfvMessage(Action.STOP, stop(virtualNetworkFunctionRecord));
            } else // Stop the VNFC Instance
            {
              nfvMessage =
                  VnfmUtils.getNfvMessageStartStop(
                      Action.STOP,
                      stopVNFCInstance(virtualNetworkFunctionRecord, vnfcInstance),
                      vnfcInstance);
            }

            break;
          }
        case RESUME:
          {
            OrVnfmGenericMessage orVnfmResumeMessage = (OrVnfmGenericMessage) message;
            virtualNetworkFunctionRecord = orVnfmResumeMessage.getVnfr();
            nsrId = virtualNetworkFunctionRecord.getParent_ns_id();

            Action resumedAction = this.getResumedAction(virtualNetworkFunctionRecord, null);
            if (orVnfmResumeMessage.getVnfrd() == null) {
              log.debug(
                  "Resuming vnfr '"
                      + virtualNetworkFunctionRecord.getId()
                      + "' for action: "
                      + resumedAction
                      + "'");
            } else {
              log.debug(
                  "Resuming vnfr '"
                      + virtualNetworkFunctionRecord.getId()
                      + "' with dependency target: '"
                      + orVnfmResumeMessage.getVnfrd().getTarget()
                      + "' for action: "
                      + resumedAction
                      + "'");
            }
            // to prevent a VNFM, that does not implement resume, from throwing Null Pointer Exception.
            if (resumedAction == null) {
              resumedAction = Action.ERROR;
            }
            nfvMessage =
                VnfmUtils.getNfvMessage(
                    resumedAction,
                    resume(virtualNetworkFunctionRecord, null, orVnfmResumeMessage.getVnfrd()));

            break;
          }
        case EXECUTE:
          {
            OrVnfmExecuteScriptMessage orVnfmExecuteMessage = (OrVnfmExecuteScriptMessage) message;
            nfvMessage =
                VnfmUtils.getNfvMessage(
                    Action.EXECUTE,
                    executeScript(
                        orVnfmExecuteMessage.getVnfr(), orVnfmExecuteMessage.getScript()));
            break;
          }
        case LOG_REQUEST:
          {
            OrVnfmLogMessage orVnfmLogMessage = (OrVnfmLogMessage) message;
            // if the VNFM does not support log requests (i.e. no LogDispatcher is implemented), it will return a default "error" OrVnfmLogMessage
            if (logDispatcher != null) {
              nfvMessage = new VnfmOrLogMessage();
              nfvMessage = logDispatcher.getLogs(orVnfmLogMessage);
            } else {
              List<String> errorList = new LinkedList<>();
              errorList.add("This VNFM does not support the requesting of log files.");
              nfvMessage = new VnfmOrLogMessage(new LinkedList<String>(), errorList);
            }
          }
      }

      log.debug(
          "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      if (nfvMessage != null) {
        return nfvMessage;
      }
    } catch (Throwable e) {
      log.error("ERROR: ", e);
      if (e instanceof VnfmSdkException) {
        VnfmSdkException vnfmSdkException = (VnfmSdkException) e;
        if (vnfmSdkException.getVnfr() != null) {
          log.debug("sending VNFR with version: " + vnfmSdkException.getVnfr().getHbVersion());
          return VnfmUtils.getNfvErrorMessage(vnfmSdkException.getVnfr(), vnfmSdkException, nsrId);
        }
      } else if (e.getCause() instanceof VnfmSdkException) {
        VnfmSdkException vnfmSdkException = (VnfmSdkException) e.getCause();
        if (vnfmSdkException.getVnfr() != null) {
          log.debug("sending VNFR with version: " + vnfmSdkException.getVnfr().getHbVersion());
          return VnfmUtils.getNfvErrorMessage(vnfmSdkException.getVnfr(), vnfmSdkException, nsrId);
        }
      }
      return VnfmUtils.getNfvErrorMessage(virtualNetworkFunctionRecord, e, nsrId);
    }
    return null;
  }

  private String getUserDataFromPackage(Set<Script> scripts) throws UnsupportedEncodingException {
    String userdata = null;
    for (Script script : scripts) {
      if (script.getName().equals("userdata.sh") || script.getName().equals("user-data.sh")) {
        ByteArrayInputStream bis = new ByteArrayInputStream(script.getPayload());
        Scanner s = new Scanner(bis).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
      }
    }
    return userdata;
  }

  private VNFCInstance getVnfcInstance(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord, VNFComponent component) {
    VNFCInstance vnfcInstance_new = null;
    boolean found = false;
    for (VirtualDeploymentUnit virtualDeploymentUnit : virtualNetworkFunctionRecord.getVdu()) {
      for (VNFCInstance vnfcInstance : virtualDeploymentUnit.getVnfc_instance()) {
        if (vnfcInstance.getVnfComponent().getId().equals(component.getId())) {
          vnfcInstance_new = vnfcInstance;
          fillProvidesVNFC(virtualNetworkFunctionRecord, vnfcInstance);
          found = true;
          log.debug("VNFComponentInstance FOUND : " + vnfcInstance_new.getVnfComponent());
          break;
        }
      }
      if (found) {
        break;
      }
    }
    return vnfcInstance_new;
  }

  private void getExtension(Map<String, String> extension) {
    log.debug("Extensions are: " + extension);

    brokerIp = extension.get("brokerIp");
    brokerPort = extension.get("brokerPort");
    monitoringIp = extension.get("monitoringIp");
    timezone = extension.get("timezone");
    username = extension.get("username");
    password = extension.get("password");
    exchangeName = extension.get("exchangeName");
    nsrId = extension.get("nsr-id");
  }

  private void setupProvides(VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {}

  private void fillProvidesVNFC(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord, VNFCInstance vnfcInstance) {}

  /**
   * This method needs to set all the parameter specified in the VNFDependency.parameters
   *
   * @param virtualNetworkFunctionRecord the {@link VirtualNetworkFunctionRecord} to fill specific
   *     provides.
   */
  protected void fillSpecificProvides(VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {}

  /**
   * This method can be overwritten in case you want a specific initialization of the
   * VirtualNetworkFunctionRecordShort from the VirtualNetworkFunctionDescriptor
   *
   * @param virtualNetworkFunctionDescriptor the {@link VirtualNetworkFunctionDescriptor} from which
   *     create the {@link VirtualNetworkFunctionRecord}
   * @param flavourId the chosen flavor
   * @param virtualLinkRecords the {@link VirtualLinkRecord} of the NSD
   * @param extension The extensions passed to the VNFManager
   * @param vimInstances the {@link Map} between vdu id and chosen VimInstances
   * @return The new {@link VirtualNetworkFunctionRecord} created
   * @throws BadFormatException in case of error while sending back to the nfvo
   * @throws NotFoundException in case of error while sending back to the nfvo
   */
  protected VirtualNetworkFunctionRecord createVirtualNetworkFunctionRecord(
      VirtualNetworkFunctionDescriptor virtualNetworkFunctionDescriptor,
      String flavourId,
      Set<VirtualLinkRecord> virtualLinkRecords,
      Map<String, String> extension,
      Map<String, Collection<BaseVimInstance>> vimInstances)
      throws BadFormatException, NotFoundException {
    try {
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord =
          VNFRUtils.createVirtualNetworkFunctionRecord(
              virtualNetworkFunctionDescriptor,
              flavourId,
              extension.get("nsr-id"),
              virtualLinkRecords,
              vimInstances);
      for (InternalVirtualLink internalVirtualLink :
          virtualNetworkFunctionRecord.getVirtual_link()) {
        for (VirtualLinkRecord virtualLinkRecord : virtualLinkRecords) {
          if (internalVirtualLink.getName().equals(virtualLinkRecord.getName())) {
            internalVirtualLink.setExtId(virtualLinkRecord.getExtId());
            internalVirtualLink.setConnectivity_type(virtualLinkRecord.getConnectivity_type());
          }
        }
      }
      log.debug("Created VirtualNetworkFunctionRecordShort: " + virtualNetworkFunctionRecord);
      return virtualNetworkFunctionRecord;
    } catch (NotFoundException | BadFormatException e) {
      e.printStackTrace();
      vnfmHelper.sendToNfvo(VnfmUtils.getNfvMessage(Action.ERROR, null));
      throw e;
    }
  }

  public abstract VirtualNetworkFunctionRecord start(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) throws Exception;

  public abstract VirtualNetworkFunctionRecord stop(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) throws Exception;

  public abstract VirtualNetworkFunctionRecord startVNFCInstance(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord, VNFCInstance vnfcInstance)
      throws Exception;

  public abstract VirtualNetworkFunctionRecord stopVNFCInstance(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord, VNFCInstance vnfcInstance)
      throws Exception;

  public abstract VirtualNetworkFunctionRecord configure(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) throws Exception;

  public abstract VirtualNetworkFunctionRecord resume(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord,
      VNFCInstance vnfcInstance,
      VNFRecordDependency dependency)
      throws Exception;

  public abstract VirtualNetworkFunctionRecord executeScript(
      VirtualNetworkFunctionRecord vnfr, Script script) throws Exception;

  protected Action getResumedAction(
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord, VNFCInstance vnfcInstance)
      throws Exception {
    return null;
  }

  protected abstract void unregister();

  protected abstract void register();

  /**
   * This method setups the VNFM and then subscribe it to the NFVO. We recommend to not change this
   * method or at least to override calling super()
   */
  protected void setup() {
    loadProperties();
    vnfmManagerEndpoint = new VnfmManagerEndpoint();
    vnfmManagerEndpoint.setType(vnfmHelper.getVnfmType());
    vnfmManagerEndpoint.setDescription(vnfmHelper.getVnfmDescription());
    vnfmManagerEndpoint.setEnabled(vnfmHelper.isVnfmEnabled());
    vnfmManagerEndpoint.setActive(true);
    vnfmManagerEndpoint.setEndpoint(vnfmHelper.getVnfmEndpoint());
    vnfmManagerEndpoint.setEndpointType(vnfmHelper.getVnfmEndpointType());
  }

  class GrantOperation implements Callable<OrVnfmGrantLifecycleOperationMessage> {
    private VirtualNetworkFunctionRecord virtualNetworkFunctionRecord;

    public VirtualNetworkFunctionRecord getVirtualNetworkFunctionRecord() {
      return virtualNetworkFunctionRecord;
    }

    public void setVirtualNetworkFunctionRecord(
        VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
      this.virtualNetworkFunctionRecord = virtualNetworkFunctionRecord;
    }

    private OrVnfmGrantLifecycleOperationMessage grantLifecycleOperation() throws VnfmSdkException {
      NFVMessage response;
      try {
        response =
            vnfmHelper.sendAndReceive(
                VnfmUtils.getNfvMessage(Action.GRANT_OPERATION, virtualNetworkFunctionRecord));
      } catch (Exception e) {
        throw new VnfmSdkException("Not able to grant operation", e, virtualNetworkFunctionRecord);
      }
      if (response != null) {
        if (response.getAction().ordinal() == Action.ERROR.ordinal()) {
          throw new VnfmSdkException(
              "Not able to grant operation because: "
                  + ((OrVnfmErrorMessage) response).getMessage(),
              ((OrVnfmErrorMessage) response).getVnfr());
        }
        OrVnfmGrantLifecycleOperationMessage orVnfmGrantLifecycleOperationMessage =
            (OrVnfmGrantLifecycleOperationMessage) response;

        return orVnfmGrantLifecycleOperationMessage;
      }
      return null;
    }

    @Override
    public OrVnfmGrantLifecycleOperationMessage call() throws Exception {
      return this.grantLifecycleOperation();
    }
  }

  class AllocateResources implements Callable<VirtualNetworkFunctionRecord> {
    private VirtualNetworkFunctionRecord virtualNetworkFunctionRecord;
    private Set<Key> keyPairs;
    private String customUserData;

    public void setVimInstances(Map<String, BaseVimInstance> vimInstances) {
      this.vimInstances = vimInstances;
    }

    private Map<String, BaseVimInstance> vimInstances;

    public VirtualNetworkFunctionRecord getVirtualNetworkFunctionRecord() {
      return virtualNetworkFunctionRecord;
    }

    public void setVirtualNetworkFunctionRecord(
        VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
      this.virtualNetworkFunctionRecord = virtualNetworkFunctionRecord;
    }

    public void setCustomUserData(String customUserData) {
      this.customUserData = customUserData;
    }

    public VirtualNetworkFunctionRecord allocateResources() throws VnfmSdkException {
      NFVMessage response;
      try {

        String userData = getUserData();
        if (customUserData != null) {
          char firstChar = customUserData.charAt(0);
          customUserData = firstChar == '\uFEFF' ? customUserData.substring(1) : customUserData;
          //          boolean customUserDataStartsWithShebang = customUserData.startsWith("#!");
          //          boolean userDataIsEmpty = userData.isEmpty();
          //          if (!customUserDataStartsWithShebang && userDataIsEmpty)
          //            throw new VnfmSdkException("Custom User Data does not have the shebang line!");
          //          else if (!userDataIsEmpty && customUserDataStartsWithShebang)
          //            throw new VnfmSdkException(
          //                "Custom User Data starts with the shebang line and you are appending it to the already existing User Data! remove the shebang line from your User Data in the package.");
          userData += customUserData;
        }
        log.debug("Userdata sent to NFVO: " + userData);
        response =
            vnfmHelper.sendAndReceive(
                VnfmUtils.getNfvInstantiateMessage(
                    virtualNetworkFunctionRecord, vimInstances, userData, keyPairs));
      } catch (Exception e) {
        log.error("" + e.getMessage());
        throw new VnfmSdkException(
            "Not able to allocate Resources", e, virtualNetworkFunctionRecord);
      }
      if (response != null) {
        if (response.getAction().ordinal() == Action.ERROR.ordinal()) {
          OrVnfmErrorMessage errorMessage = (OrVnfmErrorMessage) response;
          log.error(errorMessage.getMessage());
          virtualNetworkFunctionRecord = errorMessage.getVnfr();
          throw new VnfmSdkException(
              "Not able to allocate Resources because: " + errorMessage.getMessage(),
              virtualNetworkFunctionRecord);
        }
        OrVnfmGenericMessage orVnfmGenericMessage = (OrVnfmGenericMessage) response;
        log.debug("Received from ALLOCATE: " + orVnfmGenericMessage.getVnfr());
        return orVnfmGenericMessage.getVnfr();
      }
      return null;
    }

    @Override
    public VirtualNetworkFunctionRecord call() throws Exception {
      return this.allocateResources();
    }

    public void setKeyPairs(Set<Key> keyPairs) {
      this.keyPairs = keyPairs;
    }
  }

  protected String getUserData() {
    return "";
  }
}
