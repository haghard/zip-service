package com.gateway.server.test.unit;

import com.gateway.server.ZipService;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExampleUnitTest {

  @Test
  public void testVerticle() {
    ZipService vert = new ZipService();

    // Interrogate your classes directly....

    assertNotNull(vert);
  }
}
