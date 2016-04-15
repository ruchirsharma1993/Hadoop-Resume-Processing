#!/bin/sh

rm -rf $2
javac -cp lib/*:src ./src/PdfInputDriver.java -d bin/
javac -cp lib/*:src ./src/PdfRecordReader.java -d bin/
javac -cp lib/*:src ./src/PdfInputFormat.java -d bin/
javac -cp lib/*:src ./src/ResumeMapper.java -d bin/
javac -cp lib/*:src ./src/ResumeReducer.java -d bin/
java -cp ./lib/*:./bin PdfInputDriver $1 $2
