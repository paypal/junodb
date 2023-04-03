package com.paypal.juno.employeedashboardapp.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import com.paypal.juno.employeedashboardapp.model.Employee;
import com.paypal.juno.employeedashboardapp.service.EmployeeService;

@Controller
public class EmployeeController {

  @Autowired
  private EmployeeService employeeService;
  private static String dbname = "mysql";
  private static String cachename = "junodb";

  @GetMapping("/")
  public String viewHomePage(Model model) {
    long start = System.currentTimeMillis();
    model.addAttribute("listEmployees", employeeService.getAllEmployees());
    long end = System.currentTimeMillis();
    long timetaken = end - start;
    model.addAttribute("timetaken", timetaken);

    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    model.addAttribute("datasource", dbname);
    return "index";
  }

  @GetMapping("/showNewEmpForm")
  public String saveEmployee(Model model) {
    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    return "save_employee.html";
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

  @PostMapping("/saveEmp")
  public String saveEmployee(@ModelAttribute("employee") Employee emp) {
    employeeService.saveEmployee(emp);
    return "redirect:/";
  }

  @GetMapping("/findEmp")
  public String findEmployee(@ModelAttribute("employee") Employee emp, Model model) {
    if (emp.getId() > 0) {
      long start = System.currentTimeMillis();
      model.addAttribute("searchedEmployee", employeeService.findEmployee(emp.getId()));
      long end = System.currentTimeMillis();
      long timetaken = end - start;
      model.addAttribute("datasource", dbname);
      model.addAttribute("timetaken", timetaken);
    }
    return "search";
  }

  @GetMapping("/updateEmp/{id}")
  public String showFormForUpdateEmployee(@PathVariable(value = "id") int id, Model model) {
    // get the employee
    Employee employee = employeeService.findEmployee(id);
    // set the employee as a model attribute to pre-populate the form
    model.addAttribute("employee", employee);
    return "update_employee.html";
  }

  @GetMapping("/deleteEmp/{id}")
  public String deleteEmployee(@PathVariable(value = "id") int id, Model model) {
    employeeService.deleteEmployee(id);
    return "redirect:/";
  }
}
