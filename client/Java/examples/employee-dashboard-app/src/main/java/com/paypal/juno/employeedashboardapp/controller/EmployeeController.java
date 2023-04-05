package com.paypal.juno.employeedashboardapp.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
        return  new SimpleDateFormat("yyyy-MM-dd").parse(date);
    } catch (ParseException e) {
        return null;
    }
}

  @GetMapping("/")
  public String viewHomePage(Model model) {
    // employeeService.saveEmployee(new Employee("Joseph",1,parseDate("2014-02-14"),"808 Sunny Ridge Dr","(532)-334-3434", "josephtony@gmail.com",
    // 2000,"Austin","FTE"));
    long start = System.currentTimeMillis();
    List<Employee> empList = employeeService.getAllEmployees();
    model.addAttribute("listEmployees", empList);
    long end = System.currentTimeMillis();
    long timetaken = end - start;
    model.addAttribute("timetaken", timetaken);

    // for (Employee employee : empList) {
    //   juno.cacheRecord(employee);
    // }

    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    model.addAttribute("datasource", dbname);
    return "index";
  }

  @GetMapping("/showNewEmpForm")
  public String saveEmployee(Model model) {
    Employee employee = new Employee();
    model.addAttribute("employee", employee);
    juno.cacheRecord(employee);
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
      Employee emp1 = juno.getRecord(emp.getId());
      if(emp1 != null){
        model.addAttribute("searchedEmployee",emp1);
        model.addAttribute("datasource", cachename);
      }else {
        emp1 = employeeService.findEmployee(emp.getId());
        model.addAttribute("searchedEmployee", emp1 );
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
    // get the employee
    Employee employee = employeeService.findEmployee(id);
    // set the employee as a model attribute to pre-populate the form
    model.addAttribute("employee", employee);
    juno.cacheRecord(employee);
    return "update_employee.html";
  }

  @GetMapping("/deleteEmp/{id}")
  public String deleteEmployee(@PathVariable(value = "id") int id, Model model) {
    employeeService.deleteEmployee(id);
    juno.destroyRecord(id);
    return "redirect:/";
  }
}
