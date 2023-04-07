package com.paypal.juno.employeedashboardapp.service;

import java.util.List;

import com.paypal.juno.employeedashboardapp.model.Employee;

public interface EmployeeService {
  List<Employee> getAllEmployees();

  void addEmployee(Employee emp) throws Exception;

  void saveEmployee(Employee emp);

  Employee findEmployee(Integer id);

  void deleteEmployee(Integer id);

}
