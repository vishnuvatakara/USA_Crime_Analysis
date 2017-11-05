REGISTER '/usr/local/pig/lib/piggybank.jar';


data = LOAD '/vishnu/crimes.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',') as (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, desc: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBI_Code:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, long: chararray, location:chararray);

A = FILTER data BY type =='THEFT';

B = GROUP A BY district;

C = FOREACH B GENERATE FLATTEN(group) AS district, COUNT(A.district);

DUMP C;
