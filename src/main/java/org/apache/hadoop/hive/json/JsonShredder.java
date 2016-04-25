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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class JsonShredder {

  private final Map<String, PrintStream> files =
      new HashMap<String, PrintStream>();

  private PrintStream getFile(String name) throws IOException {
    PrintStream result = files.get(name);
    if (result == null) {
      result = new PrintStream(new FileOutputStream(name + ".txt"));
      files.put(name, result);
    }
    return result;
  }

  private void shredObject(String name, JsonElement json) throws IOException {
    if (json.isJsonPrimitive()) {
      JsonPrimitive primitive = (JsonPrimitive) json;
      getFile(name).println(primitive.getAsString());
    } else if (json.isJsonNull()) {
      // just skip it
    } else if (json.isJsonArray()) {
      for(JsonElement child: ((JsonArray) json)) {
        shredObject(name + ".list", child);
      }
    } else {
      JsonObject obj = (JsonObject) json;
      for(Map.Entry<String,JsonElement> field: obj.entrySet()) {
        String fieldName = field.getKey();
        shredObject(name + "." + fieldName, field.getValue());
      }
    }
  }

  private void close() throws IOException {
    for(Map.Entry<String, PrintStream> file: files.entrySet()) {
      file.getValue().close();
    }
  }

  public static void main(String[] args) throws Exception {
    int count = 0;
    JsonShredder shredder = new JsonShredder();
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
        shredder.shredObject("root", item);
      }
    }
    shredder.close();
    System.out.println(count + " records read");
    System.out.println();
  }
}
