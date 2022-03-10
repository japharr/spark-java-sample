package com.japharr.sample.sql;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MainTesting {
  public static void main(String[] args) {
    Set<String> set1 = new HashSet<>();
    set1.add("John"); set1.add("Jame");

    Set<String> set2 = new HashSet<>(Collections.singletonList("Jame"));

    System.out.println("intersection: " + Sets.intersection(set1, set2));
    System.out.println("union: " + Sets.union(set1, set2));
    System.out.println("difference: " + Sets.difference(set1, set2));
  }
}
