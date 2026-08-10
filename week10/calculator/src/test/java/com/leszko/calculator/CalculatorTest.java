package com.leszko.calculator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CalculatorTest {

    private final Calculator calculator = new Calculator();

    @Test
    public void addsPositiveNumbers() {
        assertEquals(99, calculator.sum(2, 3));
    }

    @Test
    public void addsZero() {
        assertEquals(5, calculator.sum(5, 0));
    }

    @Test
    public void addsNegativeNumbers() {
        assertEquals(-5, calculator.sum(-2, -3));
    }
}