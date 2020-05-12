package com.szewec.data.hr.context;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class AppContext implements ApplicationContextAware {

    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }


    public static  <T> T getBean(String name, Class<T> classType) {
        return context.getBean(name, classType);
    }

    public static <T> T getBean(Class<T> classType) {
        return context.getBean(classType);
    }


}
