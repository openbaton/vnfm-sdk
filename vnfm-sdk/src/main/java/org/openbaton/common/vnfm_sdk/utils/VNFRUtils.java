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

package org.openbaton.common.vnfm_sdk.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.openbaton.catalogue.mano.common.AutoScalePolicy;
import org.openbaton.catalogue.mano.common.DeploymentFlavour;
import org.openbaton.catalogue.mano.common.HighAvailability;
import org.openbaton.catalogue.mano.common.LifecycleEvent;
import org.openbaton.catalogue.mano.common.ScalingAction;
import org.openbaton.catalogue.mano.common.ScalingAlarm;
import org.openbaton.catalogue.mano.common.faultmanagement.Criteria;
import org.openbaton.catalogue.mano.common.faultmanagement.FaultManagementPolicy;
import org.openbaton.catalogue.mano.descriptor.InternalVirtualLink;
import org.openbaton.catalogue.mano.descriptor.VNFComponent;
import org.openbaton.catalogue.mano.descriptor.VNFDConnectionPoint;
import org.openbaton.catalogue.mano.descriptor.VirtualDeploymentUnit;
import org.openbaton.catalogue.mano.descriptor.VirtualNetworkFunctionDescriptor;
import org.openbaton.catalogue.mano.record.Status;
import org.openbaton.catalogue.mano.record.VNFCInstance;
import org.openbaton.catalogue.mano.record.VirtualLinkRecord;
import org.openbaton.catalogue.mano.record.VirtualNetworkFunctionRecord;
import org.openbaton.catalogue.nfvo.Configuration;
import org.openbaton.catalogue.nfvo.ConfigurationParameter;
import org.openbaton.catalogue.nfvo.viminstances.BaseVimInstance;
import org.openbaton.catalogue.nfvo.viminstances.OpenstackVimInstance;
import org.openbaton.common.vnfm_sdk.exception.BadFormatException;
import org.openbaton.common.vnfm_sdk.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by mob on 31.08.15. */
public class VNFRUtils {

  private static Logger log = LoggerFactory.getLogger(VNFRUtils.class);

  public static VirtualNetworkFunctionRecord createVirtualNetworkFunctionRecord(
      VirtualNetworkFunctionDescriptor vnfd,
      String flavourKey,
      String nsr_id,
      Set<VirtualLinkRecord> vlr,
      Map<String, Collection<BaseVimInstance>> vimInstances)
      throws NotFoundException, BadFormatException {
    VirtualNetworkFunctionRecord virtualNetworkFunctionRecord = new VirtualNetworkFunctionRecord();

    setBasicFields(vnfd, nsr_id, virtualNetworkFunctionRecord);

    setRequires(vnfd, virtualNetworkFunctionRecord);

    setProvides(vnfd, virtualNetworkFunctionRecord);

    setMonitoringParameters(vnfd, virtualNetworkFunctionRecord);

    setAutoScalePolicies(vnfd, virtualNetworkFunctionRecord);

    // TODO mange the VirtualLinks and links...
    //        virtualNetworkFunctionRecord.setConnected_external_virtual_link(vnfd.getVirtual_link());

    setVdu(vnfd, vimInstances, virtualNetworkFunctionRecord);

    setConnectionPoints(vnfd, virtualNetworkFunctionRecord);

    // TODO find a way to choose between deployment flavors and create the new one
    setDeploymentFlavours(vnfd, flavourKey, vimInstances, virtualNetworkFunctionRecord);

    setLifeCycleEvents(vnfd, virtualNetworkFunctionRecord);

    setInternalVirtualLinks(vnfd, vlr, virtualNetworkFunctionRecord);

    return virtualNetworkFunctionRecord;
  }

  private static void setBasicFields(
      VirtualNetworkFunctionDescriptor vnfd,
      String nsr_id,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setLifecycle_event_history(new LinkedHashSet<>());
    virtualNetworkFunctionRecord.setParent_ns_id(nsr_id);
    virtualNetworkFunctionRecord.setName(vnfd.getName());
    virtualNetworkFunctionRecord.setType(vnfd.getType());
    virtualNetworkFunctionRecord.setCyclicDependency(vnfd.hasCyclicDependency());
    setConfigurations(vnfd, virtualNetworkFunctionRecord);
    virtualNetworkFunctionRecord.setPackageId(vnfd.getVnfPackageLocation());
    if (vnfd.getEndpoint() != null) {
      virtualNetworkFunctionRecord.setEndpoint(vnfd.getEndpoint());
    } else virtualNetworkFunctionRecord.setEndpoint(vnfd.getType());
    virtualNetworkFunctionRecord.setVendor(vnfd.getVendor());
    virtualNetworkFunctionRecord.setVersion(vnfd.getVersion());
    virtualNetworkFunctionRecord.setDescriptor_reference(vnfd.getId());
    virtualNetworkFunctionRecord.setVnf_address(new HashSet<String>());
    virtualNetworkFunctionRecord.setStatus(Status.NULL);
  }

  private static void setInternalVirtualLinks(
      VirtualNetworkFunctionDescriptor vnfd,
      Set<VirtualLinkRecord> vlr,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setVirtual_link(new HashSet<InternalVirtualLink>());
    HashSet<InternalVirtualLink> internalVirtualLinks = new HashSet<>();
    for (InternalVirtualLink internalVirtualLink : vnfd.getVirtual_link()) {
      InternalVirtualLink internalVirtualLink_new = new InternalVirtualLink();
      internalVirtualLink_new.setName(internalVirtualLink.getName());

      for (VirtualLinkRecord virtualLinkRecord : vlr) {
        if (virtualLinkRecord.getName().equals(internalVirtualLink_new.getName())) {
          internalVirtualLink_new.setExtId(virtualLinkRecord.getExtId());
        }
      }

      internalVirtualLink_new.setCidr(internalVirtualLink.getCidr());
      internalVirtualLink_new.setLeaf_requirement(internalVirtualLink.getLeaf_requirement());
      internalVirtualLink_new.setRoot_requirement(internalVirtualLink.getRoot_requirement());
      internalVirtualLink_new.setConnection_points_references(new HashSet<>());
      for (String conn : internalVirtualLink.getConnection_points_references()) {
        internalVirtualLink_new.getConnection_points_references().add(conn);
      }
      internalVirtualLink_new.setQos(new HashSet<>());
      for (String qos : internalVirtualLink.getQos()) {
        internalVirtualLink_new.getQos().add(qos);
      }
      internalVirtualLink_new.setTest_access(new HashSet<>());
      for (String test : internalVirtualLink.getTest_access()) {
        internalVirtualLink_new.getTest_access().add(test);
      }
      internalVirtualLink_new.setConnectivity_type(internalVirtualLink.getConnectivity_type());
      internalVirtualLinks.add(internalVirtualLink_new);
    }
    virtualNetworkFunctionRecord.getVirtual_link().addAll(internalVirtualLinks);
  }

  private static void setLifeCycleEvents(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setLifecycle_event(new LinkedHashSet<LifecycleEvent>());
    HashSet<LifecycleEvent> lifecycleEvents = new HashSet<>();
    for (LifecycleEvent lifecycleEvent : vnfd.getLifecycle_event()) {
      LifecycleEvent lifecycleEvent_new = new LifecycleEvent();
      lifecycleEvent_new.setEvent(lifecycleEvent.getEvent());
      lifecycleEvent_new.setLifecycle_events(new ArrayList<>());
      for (String event : lifecycleEvent.getLifecycle_events()) {
        lifecycleEvent_new.getLifecycle_events().add(event);
      }
      log.debug(
          "Found SCRIPTS for EVENT "
              + lifecycleEvent_new.getEvent()
              + ": "
              + lifecycleEvent_new.getLifecycle_events().size());
      lifecycleEvents.add(lifecycleEvent_new);
    }
    virtualNetworkFunctionRecord.setLifecycle_event(lifecycleEvents);
  }

  private static void setDeploymentFlavours(
      VirtualNetworkFunctionDescriptor vnfd,
      String flavourKey,
      Map<String, Collection<BaseVimInstance>> vimInstances,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord)
      throws BadFormatException {
    virtualNetworkFunctionRecord.setDeployment_flavour_key(flavourKey);
    for (VirtualDeploymentUnit virtualDeploymentUnit : vnfd.getVdu()) {
      Collection<? extends BaseVimInstance> vimInstancesTmp =
          vimInstances.get(virtualDeploymentUnit.getId());

      if (vimInstancesTmp == null) {
        vimInstancesTmp = vimInstances.get(virtualDeploymentUnit.getName());
      }
      if (vimInstancesTmp == null) {
        vimInstancesTmp = vimInstances.get(virtualDeploymentUnit.getId());
      }
      for (BaseVimInstance vi : vimInstancesTmp) {
        for (String name : virtualDeploymentUnit.getVimInstanceName()) {
          if (name.equals(vi.getName())) {
            if (!existsDeploymentFlavor(
                virtualNetworkFunctionRecord.getDeployment_flavour_key(), vi)) {
              throw new BadFormatException(
                  "no key "
                      + virtualNetworkFunctionRecord.getDeployment_flavour_key()
                      + " found in vim instance: "
                      + vi);
            }
          }
        }
      }
    }
  }

  private static void setConnectionPoints(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setConnection_point(new HashSet<>());
    virtualNetworkFunctionRecord.getConnection_point().addAll(vnfd.getConnection_point());
  }

  private static void setVdu(
      VirtualNetworkFunctionDescriptor vnfd,
      Map<String, Collection<BaseVimInstance>> vimInstances,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setVdu(new HashSet<>());
    for (VirtualDeploymentUnit virtualDeploymentUnit : vnfd.getVdu()) {
      VirtualDeploymentUnit vduNew = new VirtualDeploymentUnit();
      vduNew.setMetadata(new HashMap<>());
      virtualDeploymentUnit.getMetadata().forEach((k, v) -> vduNew.getMetadata().put(k, v));
      vduNew.setParent_vdu(virtualDeploymentUnit.getId());
      vduNew.setName(virtualDeploymentUnit.getName());
      vduNew.setVimInstanceName(virtualDeploymentUnit.getVimInstanceName());
      vduNew.setHostname(virtualDeploymentUnit.getHostname());
      vduNew.setComputation_requirement(virtualDeploymentUnit.getComputation_requirement());
      vduNew.setScale_in_out(virtualDeploymentUnit.getScale_in_out());
      vduNew.setVdu_constraint(virtualDeploymentUnit.getVdu_constraint());
      vduNew.setVirtual_network_bandwidth_resource(
          virtualDeploymentUnit.getVirtual_network_bandwidth_resource());
      vduNew.setVirtual_memory_resource_element(
          virtualDeploymentUnit.getVirtual_memory_resource_element());

      setVnfComponents(virtualDeploymentUnit, vduNew);

      setVduLifeCycleEvents(virtualDeploymentUnit, vduNew);

      setMonitoringParameters(virtualDeploymentUnit, vduNew);

      setHighAvailability(virtualDeploymentUnit, vduNew);

      setFaultManagementPolicies(virtualDeploymentUnit, vduNew);

      setVmImages(virtualDeploymentUnit, vduNew);

      setVimInstanceNames(vimInstances, virtualDeploymentUnit, vduNew);

      virtualNetworkFunctionRecord.getVdu().add(vduNew);
    }
  }

  private static void setHighAvailability(
      VirtualDeploymentUnit vdu, VirtualDeploymentUnit vdu_new) {
    if (vdu.getHigh_availability() == null) return;
    HighAvailability highAvailability = new HighAvailability();
    highAvailability.setRedundancyScheme(vdu.getHigh_availability().getRedundancyScheme());
    highAvailability.setResiliencyLevel(vdu.getHigh_availability().getResiliencyLevel());
    vdu_new.setHigh_availability(highAvailability);
  }

  private static void setVimInstanceNames(
      Map<String, Collection<BaseVimInstance>> vimInstances,
      VirtualDeploymentUnit virtualDeploymentUnit,
      VirtualDeploymentUnit vdu_new) {
    Collection<? extends BaseVimInstance> vimInstancesTmp =
        vimInstances.get(virtualDeploymentUnit.getId());
    if (vimInstancesTmp == null) {
      vimInstancesTmp = vimInstances.get(virtualDeploymentUnit.getName());
    }

    Set<String> names = new LinkedHashSet<>();
    for (BaseVimInstance vi : vimInstancesTmp) {
      names.add(vi.getName());
    }
    vdu_new.setVimInstanceName(names);
  }

  private static void setVmImages(
      VirtualDeploymentUnit virtualDeploymentUnit, VirtualDeploymentUnit vdu_new) {
    HashSet<String> vmImages = new HashSet<>();
    vmImages.addAll(virtualDeploymentUnit.getVm_image());
    vdu_new.setVm_image(vmImages);
  }

  private static void setFaultManagementPolicies(
      VirtualDeploymentUnit virtualDeploymentUnit, VirtualDeploymentUnit vdu_new) {
    Set<FaultManagementPolicy> faultManagementPolicies = new HashSet<>();
    if (virtualDeploymentUnit.getFault_management_policy() != null) {
      log.debug(
          "Adding the fault management policies: "
              + virtualDeploymentUnit.getFault_management_policy());
      for (FaultManagementPolicy vrfmp : virtualDeploymentUnit.getFault_management_policy()) {
        FaultManagementPolicy faultManagementPolicy_new = new FaultManagementPolicy();
        faultManagementPolicy_new.setAction(vrfmp.getAction());
        Set<Criteria> criteriaSet = new HashSet<>();
        for (Criteria criteria : vrfmp.getCriteria()) {
          Criteria criteria_new = new Criteria();
          criteria_new.setName(criteria.getName());
          criteria_new.setParameter_ref(criteria.getParameter_ref());
          criteria_new.setFunction(criteria.getFunction());
          criteria_new.setVnfc_selector(criteria.getVnfc_selector());
          criteria_new.setComparison_operator(criteria.getComparison_operator());
          criteria_new.setThreshold(criteria.getThreshold());
          criteriaSet.add(criteria_new);
        }
        faultManagementPolicy_new.setCriteria(criteriaSet);
        faultManagementPolicy_new.setName(vrfmp.getName());
        faultManagementPolicy_new.setPeriod(vrfmp.getPeriod());
        faultManagementPolicy_new.setSeverity(vrfmp.getSeverity());
        faultManagementPolicy_new.setIsVNFAlarm(vrfmp.isVNFAlarm());
        faultManagementPolicies.add(faultManagementPolicy_new);
      }
    }
    vdu_new.setFault_management_policy(faultManagementPolicies);
  }

  private static void setMonitoringParameters(
      VirtualDeploymentUnit virtualDeploymentUnit, VirtualDeploymentUnit vdu_new) {
    HashSet<String> monitoringParameters = new HashSet<>();
    monitoringParameters.addAll(virtualDeploymentUnit.getMonitoring_parameter());
    vdu_new.setMonitoring_parameter(monitoringParameters);
  }

  private static void setVduLifeCycleEvents(
      VirtualDeploymentUnit virtualDeploymentUnit, VirtualDeploymentUnit vdu_new) {
    HashSet<LifecycleEvent> lifecycleEvents = new HashSet<>();
    for (LifecycleEvent lifecycleEvent : virtualDeploymentUnit.getLifecycle_event()) {
      LifecycleEvent lifecycleEvent_new = new LifecycleEvent();
      lifecycleEvent_new.setEvent(lifecycleEvent.getEvent());
      lifecycleEvent_new.setLifecycle_events(lifecycleEvent.getLifecycle_events());
      lifecycleEvents.add(lifecycleEvent_new);
    }
    vdu_new.setLifecycle_event(lifecycleEvents);
  }

  private static void setVnfComponents(
      VirtualDeploymentUnit virtualDeploymentUnit, VirtualDeploymentUnit vdu_new) {
    HashSet<VNFComponent> vnfComponents = new HashSet<>();
    for (VNFComponent component : virtualDeploymentUnit.getVnfc()) {
      VNFComponent component_new = new VNFComponent();
      HashSet<VNFDConnectionPoint> connectionPoints = new HashSet<>();
      for (VNFDConnectionPoint connectionPoint : component.getConnection_point()) {
        VNFDConnectionPoint connectionPointNew = new VNFDConnectionPoint();
        connectionPointNew.setVirtual_link_reference(connectionPoint.getVirtual_link_reference());
        connectionPointNew.setChosenPool(connectionPoint.getChosenPool());
        connectionPointNew.setVirtual_link_reference_id(
            connectionPoint.getVirtual_link_reference_id());
        connectionPointNew.setType(connectionPoint.getType());
        connectionPointNew.setFloatingIp(connectionPoint.getFloatingIp());
        connectionPointNew.setFixedIp(connectionPoint.getFixedIp());
        connectionPointNew.setInterfaceId(connectionPoint.getInterfaceId());
        if (connectionPoint.getMetadata() != null)
          connectionPoint
              .getMetadata()
              .forEach(
                  (k, v) -> {
                    connectionPointNew.setMetadata(new HashMap<>());
                    connectionPointNew.getMetadata().put(k, v);
                  });
        connectionPoints.add(connectionPointNew);
      }
      component_new.setConnection_point(connectionPoints);
      vnfComponents.add(component_new);
    }
    vdu_new.setVnfc(vnfComponents);
    vdu_new.setVnfc_instance(new HashSet<VNFCInstance>());
  }

  private static void setAutoScalePolicies(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setAuto_scale_policy(new HashSet<AutoScalePolicy>());
    for (AutoScalePolicy autoScalePolicy : vnfd.getAuto_scale_policy()) {
      AutoScalePolicy newAutoScalePolicy = new AutoScalePolicy();
      newAutoScalePolicy.setName(autoScalePolicy.getName());
      newAutoScalePolicy.setType(autoScalePolicy.getType());
      newAutoScalePolicy.setCooldown(autoScalePolicy.getCooldown());
      newAutoScalePolicy.setPeriod(autoScalePolicy.getPeriod());
      newAutoScalePolicy.setComparisonOperator(autoScalePolicy.getComparisonOperator());
      newAutoScalePolicy.setThreshold(autoScalePolicy.getThreshold());
      newAutoScalePolicy.setMode(autoScalePolicy.getMode());
      newAutoScalePolicy.setActions(new HashSet<ScalingAction>());
      for (ScalingAction action : autoScalePolicy.getActions()) {
        ScalingAction newAction = new ScalingAction();
        newAction.setValue(action.getValue());
        newAction.setType(action.getType());
        if (action.getTarget() == null || action.getTarget().equals("")) {
          newAction.setTarget(vnfd.getType());
        } else {
          newAction.setTarget(action.getTarget());
        }
        newAutoScalePolicy.getActions().add(newAction);
      }
      newAutoScalePolicy.setAlarms(new HashSet<ScalingAlarm>());
      for (ScalingAlarm alarm : autoScalePolicy.getAlarms()) {
        ScalingAlarm newAlarm = new ScalingAlarm();
        newAlarm.setComparisonOperator(alarm.getComparisonOperator());
        newAlarm.setMetric(alarm.getMetric());
        newAlarm.setStatistic(alarm.getStatistic());
        newAlarm.setThreshold(alarm.getThreshold());
        newAlarm.setWeight(alarm.getWeight());
        newAutoScalePolicy.getAlarms().add(newAlarm);
      }
      virtualNetworkFunctionRecord.getAuto_scale_policy().add(newAutoScalePolicy);
    }
  }

  private static void setMonitoringParameters(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    virtualNetworkFunctionRecord.setMonitoring_parameter(new HashSet<String>());
    virtualNetworkFunctionRecord.getMonitoring_parameter().addAll(vnfd.getMonitoring_parameter());
  }

  private static void setProvides(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    Configuration provides = new Configuration();
    provides.setConfigurationParameters(new HashSet<ConfigurationParameter>());
    provides.setName("provides");
    virtualNetworkFunctionRecord.setProvides(provides);

    if (vnfd.getProvides() != null) {
      for (String key : vnfd.getProvides()) {
        ConfigurationParameter configurationParameter = new ConfigurationParameter();
        log.debug("Adding " + key + " to provides");
        configurationParameter.setConfKey(key);
        virtualNetworkFunctionRecord
            .getProvides()
            .getConfigurationParameters()
            .add(configurationParameter);
      }
    }
  }

  private static void setRequires(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    Configuration requires = new Configuration();
    requires.setName("requires");
    requires.setConfigurationParameters(new HashSet<ConfigurationParameter>());
    virtualNetworkFunctionRecord.setRequires(requires);

    if (vnfd.getRequires() != null) {
      for (String vnfdName : vnfd.getRequires().keySet()) {
        for (String key : vnfd.getRequires().get(vnfdName).getParameters()) {
          ConfigurationParameter configurationParameter = new ConfigurationParameter();
          log.debug("Adding " + key + " to requires");
          configurationParameter.setConfKey(key);
          virtualNetworkFunctionRecord
              .getRequires()
              .getConfigurationParameters()
              .add(configurationParameter);
        }
      }
    }
  }

  private static void setConfigurations(
      VirtualNetworkFunctionDescriptor vnfd,
      VirtualNetworkFunctionRecord virtualNetworkFunctionRecord) {
    Configuration configuration = new Configuration();
    if (vnfd.getConfigurations() != null) {
      configuration.setName(vnfd.getConfigurations().getName());
    } else configuration.setName(virtualNetworkFunctionRecord.getName());

    configuration.setConfigurationParameters(new HashSet<ConfigurationParameter>());
    if (vnfd.getConfigurations() != null) {
      for (ConfigurationParameter configurationParameter :
          vnfd.getConfigurations().getConfigurationParameters()) {
        ConfigurationParameter cp = new ConfigurationParameter();
        cp.setConfKey(configurationParameter.getConfKey());
        cp.setValue(configurationParameter.getValue());
        configuration.getConfigurationParameters().add(cp);
      }
    }
    virtualNetworkFunctionRecord.setConfigurations(configuration);
  }

  private static boolean existsDeploymentFlavor(String key, BaseVimInstance vimInstance) {
    if (vimInstance instanceof OpenstackVimInstance) {
      OpenstackVimInstance openstackVimInstance = (OpenstackVimInstance) vimInstance;
      for (DeploymentFlavour deploymentFlavour : openstackVimInstance.getFlavours()) {
        if (deploymentFlavour.getFlavour_key().equals(key)
            || deploymentFlavour.getExtId().equals(key)
            || deploymentFlavour.getId().equals(key)) {
          return true;
        }
      }
      return false;
    }
    return true;
  }
}
