package com.paypal.juno.employeedashboardapp.service;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.paypal.juno.employeedashboardapp.model.Employee;
import com.paypal.juno.employeedashboardapp.repository.EmployeeRepository;

@Service
public class EmployeeServiceImpl implements EmployeeService {

  @Autowired
  private EmployeeRepository employeeRepository;

  @Override
  public List<Employee> getAllEmployees() {
    return employeeRepository.findAll();
  }

  public class EmployeeAlreadyExistsException extends RuntimeException {
    public EmployeeAlreadyExistsException(String message) {
      super(message);
    }
  }

  @Override
  public void addEmployee(Employee emp) throws Exception {
    Optional<Employee> recordInDB = employeeRepository.findById(emp.getId());
    if (recordInDB.isPresent()) {
      throw new EmployeeAlreadyExistsException(
          "Employee with id " + emp.getId() + " already exists. Data Source: MysqlDB");
    } else {
      employeeRepository.save(emp);
    }
  }

  @Override
  public void saveEmployee(Employee emp) {
    employeeRepository.save(emp);
  }

  @Override
  public Employee findEmployee(Integer id) {
    Optional<Employee> optional = employeeRepository.findById(id);
    Employee emp = null;
    if (optional.isPresent()) {
      emp = optional.get();
    }
    return emp;
  }

  @Override
  public void deleteEmployee(Integer id) {
    Optional<Employee> optional = employeeRepository.findById(id);
    if (optional.isPresent()) {
      employeeRepository.deleteById(id);
    }
  }

}
