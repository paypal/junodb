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
  public Exception saveEmployee(Employee emp) {
    try {
      Integer id = emp.getId();
      if (id != null && employeeRepository.existsById(id)) {
        return new EmployeeAlreadyExistsException("Employee with id " + id + " already exists");
      }
      employeeRepository.save(emp);
    } catch (Exception e) {
      return e;
    }
    return null;
  }

  @Override
  public Employee findEmployee(Integer id) {
    Optional<Employee> optional = employeeRepository.findById(id);
    Employee emp = null;
    if (optional.isPresent()) {
      emp = optional.get();
    }
    // } else {
    // throw new RuntimeException(" Employee not found for id :: " + id);
    // }
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
