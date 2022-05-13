package io.confluent.connect.jdbc.util;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class IdentifierRulesTest {

  private IdentifierRules rules;
  private List<String> parts;

  @Before
  public void beforeEach() {
    rules = IdentifierRules.DEFAULT;
  }

  @Test
  public void testParsingWithMultiCharacterQuotes() {
    rules = new IdentifierRules(".", "'''", "'''");
    assertParts("'''p1'''.'''p2'''.'''p3'''", "p1", "p2", "p3");
    assertParts("'''p1'''.'''p3'''", "p1", "p3");
    assertParts("'''p1'''", "p1");
    assertParts("'''p1.1.2.3'''", "p1.1.2.3");
    assertParts("'''p1.1.2.3.'''", "p1.1.2.3.");
    assertParts("", "");
    assertParsingFailure("'''p1.p2"); // unmatched quote
    assertParsingFailure("'''p1'''.'''p3'''."); // ends with delim
  }

  @Test
  public void testParsingWithDifferentLeadingAndTrailingQuotes() {
    rules = new IdentifierRules(".", "[", "]");
    assertParts("[p1].[p2].[p3]", "p1", "p2", "p3");
    assertParts("[p1].[p3]", "p1", "p3");
    assertParts("[p1]", "p1");
    assertParts("[p1.1.2.3]", "p1.1.2.3");
    assertParts("[p1[.[1.[2.3]", "p1[.[1.[2.3");
    assertParts("", "");
    assertParsingFailure("[p1].[p3]."); // ends with delim
  }

  @Test
  public void testParsingWithSingleCharacterQuotes() {
    rules = new IdentifierRules(".", "'", "'");
    assertParts("'p1'.'p2'.'p3'", "p1", "p2", "p3");
    assertParts("'p1'.'p3'", "p1", "p3");
    assertParts("'p1'", "p1");
    assertParts("'p1.1.2.3'", "p1.1.2.3");
    assertParts("", "");
    assertParsingFailure("'p1'.'p3'."); // ends with delim
  }

  @Test
  public void testParsingWithoutQuotes() {
    rules = new IdentifierRules(".", "'", "'");
    assertParts("p1.p2.p3", "p1", "p2", "p3");
    assertParts("p1.p3", "p1", "p3");
    assertParts("p1", "p1");
    assertParts("", "");
    assertParsingFailure("'p1'.'p3'."); // ends with delim
    assertParsingFailure("p1.p3."); // ends with delim
  }

  @Test
  public void testParsingWithUnsupportedQuotes() {
    rules = new IdentifierRules(".", " ", " ");
    assertParts("p1.p2.p3", "p1", "p2", "p3");
    assertParts("p1.p3", "p1", "p3");
    assertParts("p1", "p1");
    assertParts("", "");
  }

  protected void assertParts(String fqn, String...expectedParts) {
    parts = rules.parseQualifiedIdentifier(fqn);
    assertEquals(expectedParts.length, parts.size());
    int index = 0;
    for (String expectedPart : expectedParts) {
      assertEquals(expectedPart, parts.get(index++));
    }
  }

  protected void assertParsingFailure(String fqn) {
    try {
      parts = rules.parseQualifiedIdentifier(fqn);
      fail("expected parsing error");
    } catch (IllegalArgumentException e) {
      // success
    }
  }

}