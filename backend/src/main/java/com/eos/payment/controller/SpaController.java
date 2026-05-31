package com.eos.payment.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class SpaController {
    @GetMapping(value = {"/", "/dashboard"})
    public String index() {
        return "forward:/index.html";
    }
}
