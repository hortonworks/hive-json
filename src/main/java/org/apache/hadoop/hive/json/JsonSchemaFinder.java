/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.json;

import com.google.gson.*;

import java.io.*;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * This class determines the equivalent Hive schema for a group of JSON documents.
 */
@SuppressWarnings("Since15")
public class JsonSchemaFinder {
    // Java Regex
    private static final Pattern HEX_PATTERN =
            Pattern.compile("^([0-9a-fA-F][0-9a-fA-F])+$");
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("^[\"]?([0-9]{4}[-/][0-9]{2}[-/][0-9]{2})[T ]" +
                    "([0-9]{2}:[0-9]{2}:[0-9]{2})" +
                    "(([ ][-+]?[0-9]{2}([:][0-9]{2})?)|Z)?[\"]?$");
    // Named-capturing group (group name: int and fraction)
    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile("^-?(?<int>[0-9]+)([.](?<fraction>[0-9]+))?$");

    private static final int INDENT = 2;
    private static final int MAX_DECIMAL_DIGITS = 38;

    static final BigInteger MIN_LONG = new BigInteger("-9223372036854775808");
    static final BigInteger MAX_LONG = new BigInteger("9223372036854775807");

    private static String rowFormatDelimited = "row format delimited\n" +
            "  fields terminated by ','\n" + "  collection items terminated by '$'\n" +
            "  map keys terminated by '#'";

    /**
     * Check and modify the column name to ensure no conflict with hive reserved keywords.
     *
     * @param fieldName
     * @return the rectified field name.
     */
    static String fieldNameValidate(String fieldName) throws URISyntaxException, IOException {
        ClassLoader classLoader = JsonSchemaFinder.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("hive_keywords");
        // byte stream --> character stream
        InputStreamReader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String keywords = bufferedReader.readLine();
        Stream<String> stream = Arrays.stream(keywords.split(","));
        List<String> keywords_list = stream.map(x -> x.trim()).collect(Collectors.toList());
        if (keywords_list.contains(fieldName.toUpperCase()))
            return fieldName.concat("_");
        return fieldName;
    }

    static HiveType pickType(JsonElement json) throws IOException, URISyntaxException {
        if (json.isJsonPrimitive()) {
            JsonPrimitive prim = (JsonPrimitive) json;
            if (prim.isBoolean()) {
                return new BooleanType();
            } else if (prim.isNumber()) {
                Matcher matcher = DECIMAL_PATTERN.matcher(prim.getAsString());
                if (matcher.matches()) {
                    int intDigits = matcher.group("int").length();
                    String fraction = matcher.group("fraction");
                    int scale = fraction == null ? 0 : fraction.length();
                    if (scale == 0) {
                        if (intDigits < 19) {
                            long value = prim.getAsLong();
                            if (value >= -128 && value < 128) {
                                return new NumericType(HiveType.Kind.BYTE, intDigits, scale);
                            } else if (value >= -32768 && value < 32768) {
                                return new NumericType(HiveType.Kind.SHORT, intDigits, scale);
                            } else if (value >= -2147483648 && value < 2147483648L) {
                                return new NumericType(HiveType.Kind.INT, intDigits, scale);
                            } else {
                                return new NumericType(HiveType.Kind.LONG, intDigits, scale);
                            }
                        } else if (intDigits == 19) {
                            // at 19 digits, it may fit inside a long, but we need to check
                            BigInteger val = prim.getAsBigInteger();
                            if (val.compareTo(MIN_LONG) >= 0 && val.compareTo(MAX_LONG) <= 0) {
                                return new NumericType(HiveType.Kind.LONG, intDigits, scale);
                            }
                        }
                    }
                    if (intDigits + scale <= MAX_DECIMAL_DIGITS) {
                        return new NumericType(HiveType.Kind.DECIMAL, intDigits, scale);
                    }
                }
                double value = prim.getAsDouble();
                if (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE) {
                    return new NumericType(HiveType.Kind.FLOAT, 0, 0);
                } else {
                    return new NumericType(HiveType.Kind.DOUBLE, 0, 0);
                }
            } else {
                String str = prim.getAsString();
                if (TIMESTAMP_PATTERN.matcher(str).matches()) {
                    return new StringType(HiveType.Kind.TIMESTAMP);
                } else if (HEX_PATTERN.matcher(str).matches()) {
                    return new StringType(HiveType.Kind.BINARY);
                } else {
                    return new StringType(HiveType.Kind.STRING);
                }
            }
        } else if (json.isJsonNull()) {
            return new NullType();
        } else if (json.isJsonArray()) {
            ListType result = new ListType();
            result.elementType = new NullType();
            for (JsonElement child : ((JsonArray) json)) {
                HiveType sub = pickType(child);
                if (result.elementType.subsumes(sub)) {
                    result.elementType.merge(sub);
                } else if (sub.subsumes(result.elementType)) {
                    sub.merge(result.elementType);
                    result.elementType = sub;
                } else {
                    result.elementType = new UnionType(result.elementType, sub);
                }
            }
            return result;
        } else {
            JsonObject obj = (JsonObject) json;
            StructType result = new StructType();
            for (Map.Entry<String, JsonElement> field : obj.entrySet()) {
                String fieldName = field.getKey();
                fieldName = fieldNameValidate(fieldName);
                HiveType type = pickType(field.getValue());
                result.fields.put(fieldName, type);
            }
            return result;
        }
    }

    static HiveType mergeType(HiveType previous, HiveType type) {
        if (previous == null) {
            return type;
        } else if (type == null) {
            return previous;
        }
        if (previous.subsumes(type)) {
            previous.merge(type);
        } else if (type.subsumes(previous)) {
            type.merge(previous);
            previous = type;
        } else {
            previous = new UnionType(previous, type);
        }
        return previous;
    }

    static void printType(PrintStream out, HiveType type, int margin) {
        if (type == null) {
            out.print("void");
        } else if (type.kind.isPrimitive) {
            out.print(type.toString());
        } else {
            switch (type.kind) {
                case STRUCT:
                    out.println("struct <");
                    boolean first = true;
                    for (Map.Entry<String, HiveType> field :
                            ((StructType) type).fields.entrySet()) {
                        if (!first) {
                            out.println(",");
                        } else {
                            first = false;
                        }
                        for (int i = 0; i < margin; i++) {
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
                    for (HiveType child : ((UnionType) type).children) {
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

    /**
     * Print each field of the StructType to create a Hive table.
     *
     * @param out
     * @param type
     * @param tableName
     */
    static void printTopType(PrintStream out, StructType type, String tableName) {
        out.println("create table " + tableName + " (");
        boolean first = true;
        for (Map.Entry<String, HiveType> field : type.fields.entrySet()) {
            if (!first) {
                out.println(",");
            } else {
                first = false;
            }
            for (int i = 0; i < INDENT; ++i) {
                out.print(' ');
            }
            out.print(field.getKey());
            out.print(" ");
            printType(out, field.getValue(), 2 * INDENT);
        }
        out.println();
        out.println(")");
        out.println(rowFormatDelimited);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("parameter error: at least one input file followed by a table name.");
            System.exit(1);
        }
        HiveType result = null;
        boolean flat = false;
        int count = 0;
        for (int i = 0; i < args.length - 1; i++) {
            if ("-f".equals(args[i])) {
                flat = true;
            } else {
                System.err.println("Reading " + args[i]);
                System.err.flush();

                // Reader: 读取character streams
                java.io.Reader reader;

                if (args[i].endsWith(".gz")) {
                    // InputStream代表byte streams
                    reader = new InputStreamReader(new GZIPInputStream(new FileInputStream(args[i])));
                } else {
                    reader = new FileReader(args[i]);
                }
                JsonStreamParser parser = new JsonStreamParser(reader);
                while (parser.hasNext()) {
                    count += 1;
                    JsonElement item = parser.next();
                    HiveType type = pickType(item);
                    result = mergeType(result, type);
                }
            }
        }
        System.err.println(count + " records read");
        System.err.println();
        if (flat) {
            result.printFlat(System.out, "root");
        } else {
            printTopType(System.out, (StructType) result, args[args.length - 1]);
        }
    }
}
