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

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class DictionaryEncoder {

  private static class FileContext {
    private final String filename;
    private PrintStream lengthFile;
    private PrintStream dictionaryDataFile;
    private PrintStream dictionaryLengthFile;

    FileContext(String filename) {
      this.filename = filename;
    }

    PrintStream getLengthFile() throws IOException {
      if (lengthFile == null) {
        lengthFile = new PrintStream(new FileOutputStream(filename + ".len"));
      }
      return lengthFile;
    }

    PrintStream getDictionaryLengthFile() throws IOException {
      if (dictionaryLengthFile == null) {
        dictionaryLengthFile = new PrintStream(new FileOutputStream
            (filename + ".dict-len"));
      }
      return dictionaryLengthFile;
    }

    PrintStream getDictionaryDataFile() throws IOException {
      if (dictionaryDataFile == null) {
        dictionaryDataFile = new PrintStream(new FileOutputStream
            (filename + ".dict-data"));
      }
      return dictionaryDataFile;
    }

    void close() throws IOException {
      if (lengthFile != null) {
        lengthFile.close();
      }
      if (dictionaryDataFile != null) {
        dictionaryDataFile.close();
      }
      if (dictionaryLengthFile != null) {
        dictionaryLengthFile.close();
      }
    }
  }

  private static class Stripe {
    private StringRedBlackTree dictionary = new StringRedBlackTree(1000000);
    private DynamicIntArray data = new DynamicIntArray(100000);
    private final static long MAX_UNIQUE = 80;
    private boolean isDirect = false;
    private long lineCount = 0;
    private final FileContext files;

    private static class LengthPrinter implements StringRedBlackTree.Visitor {
      private final PrintStream out;
      private final int[] translation;
      int position = 0;

      LengthPrinter(PrintStream out, int size) {
        this.out = out;
        translation = new int[size];
      }

      @Override
      public void visit(StringRedBlackTree.VisitorContext context
                        ) throws IOException {
        String line = context.getString();
        out.println(line.length());
        translation[context.getOriginalPosition()] = position++;
      }
    }

    Stripe(FileContext context) {
      this.files = context;
    }

    void add(String line) {
      int addr = dictionary.add(line);
      data.add(addr);
      lineCount += 1;
    }

    void close() throws IOException {
      System.out.println("Dictionary = " + dictionary.size() + " of " +
          lineCount + " = " + (dictionary.size() * 100 / lineCount));
      if (dictionary.size() * 100 / lineCount <= MAX_UNIQUE) {
        LengthPrinter lengthPrinter =
            new LengthPrinter(files.getDictionaryLengthFile(),
                dictionary.size());
        dictionary.visit(lengthPrinter);
        PrintStream dataFile = files.getDictionaryDataFile();
        for(int i=0; i < data.size(); ++i) {
          dataFile.println(lengthPrinter.translation[data.get(i)]);
        }
      } else {
        PrintStream lenFile = files.getLengthFile();
        for(int i=0; i < data.size(); ++i) {
          lenFile.println(dictionary.get(data.get(i)).length());
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    for(String filename: args) {
      System.out.println("Processing " + filename);
      FileContext files = new FileContext(filename);
      BufferedReader in
          = new BufferedReader(new FileReader(filename));
      long lineNumber = 0;
      String line = in.readLine();
      Stripe stripe = new Stripe(files);
      while (line != null) {
        stripe.add(line);
        lineNumber += 1;
        if (lineNumber % 1000000 == 0) {
          stripe.close();
          stripe = new Stripe(files);
        }
        line = in.readLine();
      }
      stripe.close();
      files.close();
    }
  }
}
