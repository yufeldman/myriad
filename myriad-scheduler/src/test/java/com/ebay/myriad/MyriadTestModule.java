package com.ebay.myriad;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.myriad.configuration.AuxTaskConfiguration;
import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.scheduler.TaskFactory.NMTaskFactoryImpl;
import com.ebay.myriad.scheduler.TaskFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

/**
 * AbstractModule extension for UnitTests
 *
 */
public class MyriadTestModule extends AbstractModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyriadTestModule.class);
  
  @SuppressWarnings("unchecked")
  @Override
  protected void configure() {
    
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    MyriadConfiguration cfg;
    try {
      cfg = mapper.readValue(
              Thread.currentThread().getContextClassLoader().getResource("myriad-config-test-default.yml"),
              MyriadConfiguration.class);
    } catch (IOException e1) {
      LOGGER.error("IOException", e1);
      return;
    }

    if (cfg == null) {
      return;
    }
    
    MapBinder<String, TaskFactory> mapBinder
    = MapBinder.newMapBinder(binder(), String.class, TaskFactory.class);
    mapBinder.addBinding("nm").to(NMTaskFactoryImpl.class).in(Scopes.SINGLETON);
    Map<String, AuxTaskConfiguration> auxServicesConfigs = cfg.getAuxTaskConfigurations();
    for (Map.Entry<String, AuxTaskConfiguration> entry : auxServicesConfigs.entrySet()) {
      String taskFactoryClass = entry.getValue().getTaskFactoryImplName();
      try {
        Class<? extends TaskFactory> implClass = (Class<? extends TaskFactory>) Class.forName(taskFactoryClass);
        mapBinder.addBinding(entry.getKey()).to(implClass).in(Scopes.SINGLETON);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

}
