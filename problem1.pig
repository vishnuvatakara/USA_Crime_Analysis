REGISTER '/usr/local/pig/lib/piggybank.jar';

data = LOAD '/vishnu/crimes.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',' ) as (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, desc: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBI_Code:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, long: chararray, location:chararray);

A = GROUP data BY FBI_Code;

B = FOREACH A GENERATE FLATTEN(group) AS FBI_Code, COUNT(data.FBI_Code);

DUMP B;
