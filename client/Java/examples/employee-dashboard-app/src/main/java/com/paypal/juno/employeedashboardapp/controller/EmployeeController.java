package com.paypal.juno.employeedashboardapp.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import com.paypal.juno.employeedashboardapp.model.Employee;
import com.paypal.juno.employeedashboardapp.service.EmployeeService;
import com.paypal.juno.employeedashboardapp.service.JunoCache;

@Controller
public class EmployeeController {

  @Autowired
  private EmployeeService employeeService;
  private static String dbname = "mysql";

  @Autowired
  private JunoCache juno;
  private static String cachename = "junodb";

  public static java.util.Date parseDate(String date) {
    try {
      return new SimpleDateFormat("yyyy-MM-dd").parse(date);
    } catch (ParseException e) {
      return null;
    }
  }

  @GetMapping("/")
  public String viewHomePage(Model model) {
    long start = System.currentTimeMillis();
    List<Employee> empList = employeeService.getAllEmployees();
    model.addAttribute("listEmployees", empList);
    long end = System.currentTimeMillis();
    long timetaken = end - start;
    model.addAttribute("timetaken", timetaken);

    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    model.addAttribute("datasource", dbname);
    return "index";
  }

  @GetMapping("/showNewEmpForm")
  public String showAddEmployeeForm(Model model) {
    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    // juno.cacheRecord(employee);
    return "add_employee.html";
  }

  @GetMapping("/searchEmp")
  public String searchPage(Model model, @ModelAttribute("searchedEmployee") Employee searchedEmp) {
    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    if (searchedEmp.getId() > 0) {
      model.addAttribute("searchedEmployee", searchedEmp);
    } else {
      model.addAttribute("searchedEmployee", null);
    }
    return "search";
  }

  @PostMapping("/addEmp")
  public String addEmployee(Model model, @ModelAttribute("employee") Employee emp) {
    try {
      if (emp.getId() < 1){
        throw new Exception("Employee Id should be positive non-zero number");
      } else if (juno.getRecord(emp.getId()) != null) {
        throw new Exception( "Employee with id " + emp.getId() + " already exists. Data Source: JunoDB");
      } else {
        employeeService.addEmployee(emp);
        juno.cacheRecord(emp);
      }
    } catch (Exception e) {
      model.addAttribute("addEmpStatus", e.getMessage());
      return "add_employee.html";
    }
    return "redirect:/";
  }

  @PostMapping("/updEmp")
  public String updateEmployee(Model model, @ModelAttribute("employee") Employee emp) {
    try {
      boolean rc = juno.conditionalUpdate(emp);
      if(rc == true){
        employeeService.saveEmployee(emp);
      }else{
        throw new Exception( "Employee Record updated already");
      }

      juno.cacheRecord(emp);
    } catch (Exception e) {
      model.addAttribute("updateEmpStatus", e.getMessage());
      return "update_employee.html";
    }
    return "redirect:/";
  }

  @GetMapping("/findEmp")
  public String findEmployee(@ModelAttribute("employee") Employee emp, Model model) {
    if (emp.getId() > 0) {
      long start = System.currentTimeMillis();
      Employee emp1 = juno.getRecord(emp.getId());
      if (emp1 != null) {
        model.addAttribute("searchedEmployee", emp1);
        model.addAttribute("datasource", cachename);
      } else {
        emp1 = employeeService.findEmployee(emp.getId());
        model.addAttribute("searchedEmployee", emp1);
        model.addAttribute("datasource", dbname);
        juno.cacheRecord(emp1);
      }
      long end = System.currentTimeMillis();
      long timetaken = end - start;
      model.addAttribute("timetaken", timetaken);
    }
    return "search";
  }

  @GetMapping("/updateEmp/{id}")
  public String showFormForUpdateEmployee(@PathVariable(value = "id") int id, Model model) {

    //Check if the Employee is already present in Cache
    Employee empLocal = juno.getRecord(id);

    if(empLocal == null){
      // get the employee from Mysql DB
      Employee employee = employeeService.findEmployee(id);
      // Add employee to Cache
      juno.cacheRecord(employee);
      // Fetch the record from Juno
      empLocal = juno.getRecord(id);
      // set the employee as a model attribute to pre-populate the form
      model.addAttribute("employee", employee);
    }else{
        // set the employee as a model attribute to pre-populate the form
        model.addAttribute("employee", empLocal);
    }

    return "update_employee.html";
  }

  @GetMapping("/deleteEmp/{id}")
  public String deleteEmployee(@PathVariable(value = "id") int id, Model model) {
    employeeService.deleteEmployee(id);
    juno.destroyRecord(id);
    return "redirect:/";
  }
}
