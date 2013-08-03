/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class JsonSchemaFinder {
  private static final Pattern HEX_PATTERN =
      Pattern.compile("^([0-9a-fA-F][0-9a-fA-F])*$");
  private static final Pattern DATE_PATTERN =
      Pattern.compile("^[\"]?([0-9]{4}[-/][0-9]{2}[-/][0-9]{2})[T ]" +
          "([0-9]{2}:[0-9]{2}:[0-9]{2})" +
          "([ ]?([-+][0-9]{2}[:]?[0-9]{2})|Z)[\"]?$");
  private static final int INDENT = 3;

  private static abstract class HiveType {
    static enum Kind {NULL, BOOLEAN, INTEGER, FLOATING_POINT, STRING,
      BINARY, TIMESTAMP, STRUCT, LIST, UNION}
    Kind kind;
    HiveType(Kind kind) {
      this.kind = kind;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || other.getClass() != getClass()) {
        return false;
      }
      return ((HiveType) other).kind.equals(kind);
    }

    @Override
    public int hashCode() {
      return kind.hashCode();
    }

    boolean isStringType() {
      switch (kind) {
        case STRING:
        case TIMESTAMP:
        case BINARY:
          return true;
        default:
          return false;
      }
    }
  }

  private static class NullType extends HiveType {
    NullType() {
      super(Kind.NULL);
    }

    @Override
    public String toString() {
      return "null";
    }
  }

  private static class BooleanType extends HiveType {
    BooleanType() {
      super(Kind.BOOLEAN);
    }

    @Override
    public String toString() {
      return "boolean";
    }
  }

  private static class IntegerType extends HiveType {
    long minValue;
    long maxValue;

    IntegerType(long value) {
      super(Kind.INTEGER);
      minValue = value;
      maxValue = value;
    }

    @Override
    public String toString() {
      if (minValue >= Byte.MIN_VALUE && maxValue <= Byte.MAX_VALUE){
        return "tinyint";
      } else if (minValue >= Short.MIN_VALUE && maxValue <= Short.MAX_VALUE) {
        return "smallint";
      } else if (minValue >= Integer.MIN_VALUE &&
          maxValue <= Integer.MAX_VALUE) {
        return "int";
      } else {
        return "bigint";
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || other.getClass() != IntegerType.class) {
        return false;
      }

      IntegerType that = (IntegerType) other;
      return this.minValue == that.minValue && this.maxValue == that.maxValue;
    }
  }

  private static class FloatingPointType extends HiveType {
    double maxValue;
    double minValue;

    FloatingPointType(double value) {
      super(Kind.FLOATING_POINT);
      this.maxValue = value;
      this.minValue = value;
    }

    @Override
    public String toString() {
      double max = Math.max(Math.abs(maxValue), Math.abs(minValue));
      if (max > Float.MAX_VALUE) {
        return "double";
      } else {
        return "float";
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || other.getClass() != FloatingPointType.class) {
        return false;
      }

      FloatingPointType that = (FloatingPointType) other;
      return this.minValue == that.minValue && this.maxValue == that.maxValue;
    }
  }

  private static class StringType extends HiveType {
    StringType() {
      super(Kind.STRING);
    }

    @Override
    public String toString() {
      return "string";
    }
  }

  private static class BinaryType extends HiveType {
    BinaryType() {
      super(Kind.BINARY);
    }

    @Override
    public String toString() {
      return "binary";
    }
  }

  private static class TimestampType extends HiveType {
    TimestampType() {
      super(Kind.TIMESTAMP);
    }

    @Override
    public String toString() {
      return "timestamp";
    }
  }

  private static class StructType extends HiveType {
    final Map<String, HiveType> fields = new TreeMap<String, HiveType>();
    final Set<String> values = new HashSet<String>();
    StructType() {
      super(Kind.STRUCT);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder("struct<");
      boolean first = true;
      for(Map.Entry<String,HiveType> field: fields.entrySet()) {
        if (!first) {
          buf.append(',');
        } else {
          first = false;
        }
        buf.append(field.getKey());
        buf.append(':');
        buf.append(field.getValue().toString());
      }
      buf.append(">");
      return buf.toString();
    }

    private boolean fieldsMatch(StructType other) {
      if (fields.size() != other.fields.size()) {
        return false;
      }
      for(Map.Entry<String, HiveType> field: fields.entrySet()) {
        HiveType otherField = other.fields.get(field.getKey());
        if (!field.getValue().equals(otherField)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) && fieldsMatch((StructType) other);
    }
  }

  private static class ListType extends HiveType {
    HiveType elementType;
    ListType() {
      super(Kind.LIST);
    }
    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder("list<");
      buf.append(elementType.toString());
      buf.append(">");
      return buf.toString();
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) &&
          elementType.equals(((ListType) other).elementType);
    }
  }

  private static class UnionType extends HiveType {
    List<HiveType> children = new ArrayList<HiveType>();
    UnionType() {
      super(Kind.UNION);
    }
    UnionType(HiveType left, HiveType right) {
      super(Kind.UNION);
      children.add(left);
      children.add(right);
    }

    void addType(HiveType type) {
      if (!children.contains(type)) {
        children.add(type);
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder("union<");
      boolean first = true;
      for(HiveType child: children) {
        if (!first) {
          buf.append(',');
        } else {
          first = false;
        }
        buf.append(child.toString());
      }
      buf.append(">");
      return buf.toString();
    }

    private boolean childrenMatch(UnionType other) {
      if (children.size() != other.children.size()) {
        return false;
      }
      for(HiveType child: children) {
        if (!other.children.contains(child)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) && childrenMatch((UnionType) other);
    }
  }

  private static HiveType pickType(JsonElement json) {
    if (json.isJsonPrimitive()) {
      JsonPrimitive prim = (JsonPrimitive) json;
      if (prim.isBoolean()) {
        return new BooleanType();
      } else if (prim.isNumber()) {
        BigDecimal dec = prim.getAsBigDecimal();
        if (dec.scale() > 0) {
          return new FloatingPointType(dec.doubleValue());
        } else {
          return new IntegerType(dec.longValue());
        }
      } else {
        String str = prim.getAsString();
        if (DATE_PATTERN.matcher(str).matches()) {
          return new TimestampType();
        } else if (HEX_PATTERN.matcher(str).matches()) {
          return new BinaryType();
        } else {
          return new StringType();
        }
      }
    } else if (json.isJsonNull()) {
      return new NullType();
    } else if (json.isJsonArray()) {
      ListType result = new ListType();
      for(JsonElement child: ((JsonArray) json)) {
        HiveType sub = pickType(child);
        if (result.elementType == null) {
          result.elementType = sub;
        } else {
          result.elementType = mergeType(result.elementType, sub);
        }
      }
      return result;
    } else {
      JsonObject obj = (JsonObject) json;
      StructType result = new StructType();
      for(Map.Entry<String,JsonElement> field: obj.entrySet()) {
        String fieldName = field.getKey();
        HiveType type = pickType(field.getValue());
        result.fields.put(fieldName, type);
      }
      StringBuilder builder = new StringBuilder();
      boolean first = true;
      for(String key: result.fields.keySet()) {
        if (first) {
          first = false;
        } else {
          builder.append(",");
        }
        builder.append(key);
      }
      result.values.add(builder.toString());
      return result;
    }
  }

  private static void mergeUnionChildType(List<HiveType> left, HiveType right) {
    for(int i=0; i < left.size(); ++i) {
      HiveType child = left.get(i);
      if (child.kind == right.kind) {
        left.set(i, mergeSameType(child, right));
        return;
      }
    }
    left.add(right);
  }

  private static HiveType mergeSameType(HiveType left, HiveType right) {
    switch (left.kind) {
      case NULL:
      case BOOLEAN:
      case STRING:
      case BINARY:
      case TIMESTAMP:
        return left;
      case INTEGER:
        IntegerType leftInt = (IntegerType) left;
        IntegerType rightInt = (IntegerType) right;
        leftInt.minValue = Math.min(leftInt.minValue, rightInt.minValue);
        leftInt.maxValue = Math.max(leftInt.minValue, rightInt.maxValue);
        return leftInt;
      case FLOATING_POINT:
        FloatingPointType leftFloat = (FloatingPointType) left;
        FloatingPointType rightFloat = (FloatingPointType) right;
        leftFloat.maxValue = Math.max(leftFloat.maxValue,
            rightFloat.maxValue);
        leftFloat.minValue = Math.min(leftFloat.minValue,
            rightFloat.minValue);
        return leftFloat;
      case STRUCT:
        StructType leftStruct = (StructType) left;
        StructType rightStruct = (StructType) right;
        for(Map.Entry<String,HiveType> rightField:
            rightStruct.fields.entrySet()) {
          String key = rightField.getKey();
          if (leftStruct.fields.containsKey(key)) {
            leftStruct.fields.put(key,
                mergeType(leftStruct.fields.get(key), rightField.getValue()));
          } else {
            leftStruct.fields.put(key, rightField.getValue());
          }
        }
        leftStruct.values.addAll(rightStruct.values);
        return leftStruct;
      case UNION:
        UnionType leftUnion = (UnionType) left;
        UnionType rightUnion = (UnionType) right;
        for(HiveType rightChild: rightUnion.children) {
          mergeUnionChildType(leftUnion.children, rightChild);
        }
        return leftUnion;
      case LIST:
        ListType leftList = (ListType) left;
        ListType rightList = (ListType) right;
        leftList.elementType = mergeType(leftList.elementType,
            rightList.elementType);
        return leftList;
      default:
        throw new IllegalArgumentException("Unknown type: " + left.kind);
    }
  }

  private static HiveType mergeType(HiveType previous, HiveType type) {
    if (previous == null) {
      return type;
    } else if (type == null) {
      return previous;
    }
    if (previous.kind == type.kind) {
      return mergeSameType(previous, type);
    }
    if (previous.kind.ordinal() > type.kind.ordinal()) {
      HiveType tmp = previous;
      previous = type;
      type = tmp;
    }

    switch (previous.kind) {
      case NULL:
        return type;
      case INTEGER:
        if (type.kind == HiveType.Kind.FLOATING_POINT) {
          IntegerType leftInt = (IntegerType) previous;
          FloatingPointType rightFloat = (FloatingPointType) type;
          rightFloat.minValue = Math.min(rightFloat.minValue, leftInt.minValue);
          rightFloat.maxValue = Math.max(rightFloat.maxValue, leftInt.maxValue);
          return rightFloat;
        } else {
          return new UnionType(previous, type);
        }
      case STRING:
        if (type.kind == HiveType.Kind.BINARY) {
          return previous;
        }
      case BOOLEAN:
      case FLOATING_POINT:
      case BINARY:
      case LIST:
      case STRUCT:
      case TIMESTAMP:
        if (type.kind == HiveType.Kind.UNION) {
          ((UnionType) type).addType(previous);
          return type;
        } else {
          return new UnionType(previous, type);
        }
      default:
        throw new IllegalArgumentException("Unknown type: " + previous.kind);
    }
  }

  private static void printType(PrintStream out, HiveType type, int margin) {
    if (type == null) {
      out.print("VOID");
    } else {
      switch (type.kind) {
        case BINARY:
        case BOOLEAN:
        case FLOATING_POINT:
        case INTEGER:
        case NULL:
        case STRING:
        case TIMESTAMP:
          out.print(type.toString());
          break;
        case STRUCT:
          out.println("struct <");
          boolean first = true;
          for(Map.Entry<String, HiveType> field:
              ((StructType) type).fields.entrySet()) {
            if (!first) {
              out.println(",");
            } else {
              first = false;
            }
            for(int i=0; i < margin; i++) {
              out.print(' ');
            }
            out.print(field.getKey());
            out.print(": ");
            printType(out, field.getValue(), margin + INDENT);
          }
          out.print(">");
          break;
        case LIST:
          out.print("array <");
          printType(out, ((ListType) type).elementType, margin + INDENT);
          out.print(">");
          break;
        case UNION:
          out.print("uniontype <");
          first = true;
          for(HiveType child: ((UnionType) type).children) {
            if (!first) {
              out.print(',');
            } else {
              first = false;
            }
            printType(out, child, margin + INDENT);
          }
          out.print(">");
          break;
        default:
          throw new IllegalArgumentException("Unknown kind " + type.kind);
      }
    }
  }

  private static void printTopType(PrintStream out, StructType type) {
    out.println("create table tbl (");
    boolean first = true;
    for(Map.Entry<String, HiveType> field: type.fields.entrySet()) {
      if (!first) {
        out.println(",");
      } else {
        first = false;
      }
      for(int i=0; i < INDENT; ++i) {
        out.print(' ');
      }
      out.print(field.getKey());
      out.print(" ");
      printType(out, field.getValue(), 2 * INDENT);
    }
    out.println();
    out.println(")");
  }

  public static void main(String[] args) throws Exception {
    HiveType result = null;
    int count = 0;
    for (String filename: args) {
      System.out.println("Reading " + filename);
      System.out.flush();
      java.io.Reader reader;
      if (filename.endsWith(".gz")) {
        reader = new InputStreamReader(new GZIPInputStream(new FileInputStream(filename)));
      } else {
        reader = new FileReader(filename);
      }
      JsonStreamParser parser = new JsonStreamParser(reader);
      while (parser.hasNext()) {
        count += 1;
        JsonElement item = parser.next();
        HiveType type = pickType(item);
        result = mergeType(result, type);
      }
    }
    System.out.println(count + " records read");
    System.out.println();
    printTopType(System.out, (StructType) result);
  }
}
