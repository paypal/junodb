//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
package com.paypal.juno;

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.stereotype.Component;

@Component
public class JunoClientBeanFactoryPostProcessor implements BeanFactoryPostProcessor {

    private Logger logger = LoggerFactory.getLogger(JunoClientBeanFactoryPostProcessor.class);
    private List<Class<?>> junoClients = new ArrayList<>(Arrays.asList(JunoClient.class, JunoAsyncClient.class, JunoReactClient.class));

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory factory) throws BeansException {
        
    	// Find the Configuration bean
    	Configuration config = factory.getBean(Configuration.class);
        JunoPropertiesProvider provider;
        List<String> qualifiedBeanList = new ArrayList<>();
        List<String> nonQualifiedBeanList= new ArrayList<>();

        //Scan Juno Client injection points, create bean and register it
        for (Class<?> junoClass: junoClients) {
            qualifiedBeanList.clear();
            nonQualifiedBeanList.clear();
            registerBean(factory,config,qualifiedBeanList,nonQualifiedBeanList,junoClass);
        }
    }

    /**
     *
     */
    private void registerBean(ConfigurableListableBeanFactory factory,Configuration config,
                                List<String> qualifiedBeanList, List<String> nonQualifiedBeanList, Class<?> junoClass){
        JunoPropertiesProvider provider;
        locateInjectionPoints(factory,qualifiedBeanList,nonQualifiedBeanList,junoClass);
        // create all beans required for each named injection point
        for (String qualifiedBean : qualifiedBeanList) {
            provider = getJunoPropertiesProvider(qualifiedBean, config);
            provider.setConfig(config);
            if(logger.isDebugEnabled()){
                String msg = String.format("Configuring %s for @Named(%s)  with the following properties: %s",junoClass.getName(),qualifiedBean,provider.toString());
                logger.debug(msg);
            }
            Object junoClient = createJunoBean(provider,junoClass);
            ((DefaultListableBeanFactory)factory).registerSingleton(qualifiedBean, junoClient);
        }

        //Create a single bean for rest of all non qualified beans
        if(!nonQualifiedBeanList.isEmpty()){
            //For non qualified bean just create one object and add
            provider = getJunoPropertiesProvider(null, config);
            provider.setConfig(config);
            if(logger.isDebugEnabled()){
                String msg = String.format("Configuring %s without @Named annotations with the following properties: %s",junoClass.getName(),provider.toString());
                logger.debug(msg);
            }
            Object junoClient = createJunoBean(provider,junoClass);
            for (String nonQualifedBean : nonQualifiedBeanList){
                ((DefaultListableBeanFactory)factory).registerSingleton(nonQualifedBean, junoClient);
            }
        }
    }

    private Object createJunoBean(JunoPropertiesProvider provider,Class<?> junoClass){
        if(junoClass == JunoClient.class){
            return JunoClientFactory.newJunoClient(provider);
        }else if(junoClass == JunoAsyncClient.class){
            return JunoClientFactory.newJunoAsyncClient(provider);
        }else if(junoClass == JunoReactClient.class){
            return JunoClientFactory.newJunoReactClient(provider);
        }else {
            return null;
        }
    }
    /**
     * Locate beans that have @Inject or @Autowired injection points for the JunoClient.  @Named
     * is also considered for qualifying injection points.
     * @param factory
     * @return
     * @throws BeansException
     */
    private void locateInjectionPoints(ConfigurableListableBeanFactory factory,List<String> qualifiedBean, 
    									List<String> nonQualifiedBean,Class<?> junoClientClass) throws BeansException {

        // get a list of all the beans in the system
        for (String beanName : factory.getBeanDefinitionNames()) {
        	
            Class<?> clazz = null;
            try {
                String beanClassName = factory.getBeanDefinition(beanName).getBeanClassName();
                if (beanClassName == null) {
                    continue;
                }
                clazz = Class.forName(beanClassName);
            } catch (ClassNotFoundException e) {
                logger.error("Unable to load injection bean", e);
                throw new BeanDefinitionValidationException("Unable to load injection bean", e);
            }

            //For Field Injection
            Field[] fields = clazz.getDeclaredFields();
            //clazz.getSuperclass()
            if (fields != null && fields.length != 0 ) {
                for (Field field : fields) {
                    Inject inject = field.getAnnotation(Inject.class);
                    Autowired autowired = field.getAnnotation(Autowired.class);
                    Named named = field.getAnnotation(Named.class);
                    if ((inject != null || autowired != null) && field.getType() == junoClientClass) {
                        populateBeanLists(qualifiedBean,nonQualifiedBean,named,field.getName());
                    }
                }
            }

            //For Field Injection of superClass
            if(clazz.getSuperclass() != null) {
                fields = clazz.getSuperclass().getDeclaredFields();
                if (fields != null && fields.length != 0) {
                    for (Field field : fields) {
                        Inject inject = field.getAnnotation(Inject.class);
                        Autowired autowired = field.getAnnotation(Autowired.class);
                        Named named = field.getAnnotation(Named.class);
                        if ((inject != null || autowired != null) && field.getType() == junoClientClass) {
                            populateBeanLists(qualifiedBean, nonQualifiedBean, named, field.getName());
                        }
                    }
                }
            }

            //For Constructor Injection
            Constructor[] Constructors = clazz.getConstructors();
            if (Constructors != null && Constructors.length != 0) {
                for (Constructor constructor : Constructors) {
                    Parameter[] params = constructor.getParameters();
                    for(Parameter param : params){
                        if(param.getType() == junoClientClass) {
                            Annotation[] annotate = param.getAnnotations();
                            if(annotate != null && annotate.length != 0) {
                                for (Annotation annotation : annotate) {
                                    if (annotation.annotationType() == Named.class) {
                                        populateBeanLists(qualifiedBean, nonQualifiedBean, ((Named) annotation), null);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Populate the bean list based on the value How they are declared. If they are declared with @named annotations
     * then the named value will be put in the qualified annotation and if there is not @Named annotation then the 
     * objects name will be put in nonQualified Bean list.
     * @param qualifiedBeanList - List of unique named annotations
     * @param nonQualifiedBeanList - List of unique Object names
     * @param qualifierName - Value of @Named annotation
     * @param beanName - Object name specified after @Inject
     */
    void populateBeanLists(List<String> qualifiedBeanList,List<String> nonQualifiedBeanList,Named qualifierName,String beanName){
        if (qualifierName == null) {
        	// Add bean name to non qualified list only if not present
        	if(!nonQualifiedBeanList.contains(beanName)){
        		nonQualifiedBeanList.add(beanName);
        	}
        } else {
            // Add qualifier to qualified list only if not present
        	if(!qualifiedBeanList.contains(qualifierName.value())){
        		qualifiedBeanList.add(qualifierName.value());
        	}
        }
    }
	
    /**
     * Look at the properties contained within the Commons Configuration for Juno client
     * properties.  There can be multiple instances.
     * <p/>
     * <p/>
     * Juno configuration properties as well as defaults will have the form:
     * Juno.connection.retries=1
     * <p/>
     * Juno configuration properties for a named/qualified injection point will have the form of
     * <qualifier>.juno.connection.retries=1
     * <p/>
     * Cases:
     * <p/>
     * - A default configuration
     * - A named configuration
     * - A named configuration for which missing properties will come from a default property name
     *
     * @return A map of JunoPropertiesProvider instances keyed by a String value representing the injection point
     *         name which will become the created bean name.
     */
    private JunoPropertiesProvider getJunoPropertiesProvider(String qualifier, Configuration configuration) {
        Properties properties = new Properties();
        JunoPropertiesProvider propertiesProvider = null;
        /* Must check specifically for unqualified and qualified cases of juno property values that match
           juno. or <qualifier>.juno.
        */
        Iterator<String> keys = null;
        if (qualifier == null) {
            // UNQAULIFIED
            //Add prefix to the property
            properties.put("prefix","");
            // Get keys prefixed with "juno"
            keys = configuration.getKeys("juno");
            // Look for the property key as is
            while (keys.hasNext()) {
                String key = (String) keys.next();
                properties.put(key, configuration.getString(key));
            }
        } else {
            // QUALIFIED
            properties.put("prefix",qualifier);
            // Get the subset of keys prefixed by the qualifier which also removes the qualifier from the property key
            Configuration subset = configuration.subset(qualifier);
            keys = subset.getKeys("juno");
            while (keys.hasNext()) {
                String key = (String)keys.next();
                properties.put(key, subset.getString(key));
            }
        }
        if (properties.size() == 0)
            throw new JunoException("No Juno Client properties could be found for qualifying properties: " + qualifier);

        propertiesProvider = new JunoPropertiesProvider(properties);
        return propertiesProvider;
    }
    
}
