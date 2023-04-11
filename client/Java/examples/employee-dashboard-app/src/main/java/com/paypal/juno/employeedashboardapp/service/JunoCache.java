package com.paypal.juno.employeedashboardapp.service;

import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.employeedashboardapp.model.Employee;
import com.paypal.juno.exception.JunoException;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.springframework.stereotype.Service;

@Service
public class JunoCache {
    private JunoClient junoClient;
    private RecordContext rctx;


    JunoCache() throws IOException {
        URL url = JunoCache.class.getResource("/application.properties");
        Properties pConfig = new Properties();
        pConfig.load(url.openStream());
        junoClient = JunoClientFactory.newJunoClient(url);
    }

    public void clearRctx(){
        rctx = null;
    }

    public boolean cacheRecord(Employee emp) {
        try {
            JunoResponse jr = junoClient.set((new Integer(emp.getId())).toString().getBytes(), Employee.serializeObject(emp));
            if(jr.getStatus() == OperationStatus.Success || jr.getStatus() == OperationStatus.UniqueKeyViolation){
                return true;
            }else {
                return false;
            }
        } catch (JunoException je) {
            return false;
        } catch (Exception e){
            return false;
        }
    }

    public boolean createRecord(Employee emp) {
        try {
            JunoResponse jr = junoClient.create((new Integer(emp.getId())).toString().getBytes(), Employee.serializeObject(emp));
            if(jr.getStatus() == OperationStatus.Success || jr.getStatus() == OperationStatus.UniqueKeyViolation){
                return true;
            }else {
                return false;
            }
        } catch (JunoException je) {
            return false;
        } catch (Exception e){
            return false;
        }
    }

    public boolean updateRecord(Employee emp) {
        try {
            JunoResponse jr = junoClient.update((new Integer(emp.getId())).toString().getBytes(), Employee.serializeObject(emp));
            if(jr.getStatus() == OperationStatus.Success){
                return true;
            }else {
                return false;
            }
        } catch (JunoException je) {
            return false;
        } catch (Exception e){
            return false;
        }
    }

    public boolean conditionalUpdate(Employee emp) {
        try {
            JunoResponse jr = junoClient.compareAndSet(rctx, Employee.serializeObject(emp),100);
            if(jr.getStatus() == OperationStatus.Success){
                return true;
            }else {
                return false;
            }
        } catch (JunoException je) {
            return false;
        } catch (Exception e){
            return false;
        }
    }

    public Employee getRecord(int id) {
        try {
            JunoResponse jr = junoClient.get((new Integer(id)).toString().getBytes(),10);
            if(jr.getStatus() == OperationStatus.Success){
                Employee emp = Employee.deserializeObject(jr.getValue());
                rctx = jr.getRecordContext();
                return emp;
            }else {
                return null;
            }
        } catch (JunoException je) {
            return null;
        } catch (Exception e){
            return null;
        }
    }

    public boolean destroyRecord(int id) {
        try {
            JunoResponse jr = junoClient.delete((new Integer(id)).toString().getBytes());
            if(jr.getStatus() == OperationStatus.Success){
                return true;
            }else {
                return false;
            }
        } catch (JunoException je) {
            return false;
        } catch (Exception e){
            return false;
        }
    }

}
