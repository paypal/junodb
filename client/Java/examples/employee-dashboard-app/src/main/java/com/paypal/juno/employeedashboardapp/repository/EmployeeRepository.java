package com.paypal.juno.employeedashboardapp.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.paypal.juno.employeedashboardapp.model.Employee;

public interface EmployeeRepository extends JpaRepository<Employee, Integer> {

}
