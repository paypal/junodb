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
package com.juno.samples.api;

import com.paypal.juno.exception.JunoException;
import java.io.UnsupportedEncodingException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


/**
 * Sample interface that shows how a service can be implemented with a separate
 * interface. This interface can be used as programmatic contract.
 *
 */
@RestController
@RequestMapping("/samplejuno")
public interface JunoClientSampleResource {

    @GetMapping("/hello")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> sayHello() throws UnsupportedEncodingException;

    @POST
    @PostMapping("/recordcreate")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> recordCreate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @POST
    @PostMapping("/recordcreatettl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.APPLICATION_JSON })
    ResponseEntity<String> recordCreate(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/recordget/{key}")
    ResponseEntity<String> recordGet(@PathVariable String key) throws JunoException, InterruptedException;

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/recordgetttl/{key}/{ttl}")
    ResponseEntity<String> recordGet(@PathVariable("key") String key, @PathVariable("ttl") Long ttl) throws JunoException, InterruptedException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordupdate")
    ResponseEntity<String> recordUpdate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordupdatettl")
    ResponseEntity<String> recordUpdate(@FormParam("key")  String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordset")
    ResponseEntity<String> recordSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordsetttl")
    ResponseEntity<String> recordSet(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;


    @POST
    @PostMapping("/recordcompareandset")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> recordCompareAndSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


    @POST
    @PostMapping("/recordcompareandsetttl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> recordCompareAndSetTTL(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @DELETE
    @Produces({ MediaType.APPLICATION_JSON })
    @DeleteMapping("/recorddelete/{key}")
    ResponseEntity<String> recordDelete(@PathVariable String key) throws JunoException;

    @POST
    @PostMapping("/reactcreate")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> reactCreate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @POST
    @PostMapping("/reactcreatettl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.APPLICATION_JSON })
    ResponseEntity<String> reactCreate(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/reactget/{key}")
    ResponseEntity<String> reactGet(@PathVariable String key) throws JunoException, InterruptedException;

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/reactgetttl/{key}/{ttl}")
    ResponseEntity<String> reactGet(@PathVariable("key") String key, @PathVariable("ttl") Long ttl) throws JunoException, InterruptedException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactupdate")
    ResponseEntity<String> reactUpdate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactupdatettl")
    ResponseEntity<String> reactUpdate(@FormParam("key")  String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactset")
    ResponseEntity<String> reactSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactsetttl")
    ResponseEntity<String> reactSet(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;


    @POST
    @PostMapping("/reactcompareandset")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> reactCompareAndSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


    @POST
    @PostMapping("/reactcompareandsetttl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> reactCompareAndSetTTL(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @DELETE
    @Produces({ MediaType.APPLICATION_JSON })
    @DeleteMapping("/reactdelete/{key}")
    ResponseEntity<String> reactDelete(@PathVariable String key) throws JunoException;

    @POST
    @PostMapping("/asynccreate")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> asyncCreate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @POST
    @PostMapping("/asynccreatettl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.APPLICATION_JSON })
    ResponseEntity<String> asyncCreate(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/asyncget/{key}")
    ResponseEntity<String> asyncGet(@PathVariable String key) throws JunoException, InterruptedException;

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/asyncgetttl/{key}/{ttl}")
    ResponseEntity<String> asyncGet(@PathVariable("key") String key, @PathVariable("ttl") Long ttl) throws JunoException, InterruptedException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncupdate")
    ResponseEntity<String> asyncUpdate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncupdatettl")
    ResponseEntity<String> asyncUpdate(@FormParam("key")  String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncset")
    ResponseEntity<String> asyncSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncsetttl")
    ResponseEntity<String> asyncSet(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;


    @POST
    @PostMapping("/asynccompareandset")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> asyncCompareAndSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


    @POST
    @PostMapping("/asynccompareandsetttl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> asyncCompareAndSetTTL(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

    @DELETE
    @Produces({ MediaType.APPLICATION_JSON })
    @DeleteMapping("/asyncdelete/{key}")
    ResponseEntity<String> asyncDelete(@PathVariable String key) throws JunoException;
}
