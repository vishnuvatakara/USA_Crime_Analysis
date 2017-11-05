REGISTER '/usr/local/pig/lib/piggybank.jar';

data = LOAD '/vishnu/crimes.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',') as (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, desc: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBI_Code:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, long: chararray, location:chararray);

A = FOREACH data GENERATE ToDate(SUBSTRING(date,0,10), 'MM/dd/yyyy') as (dt:datetime);

B1 = FILTER A BY DaysBetween(dt,(datetime)ToDate('10/01/2014', 'MM/dd/yyyy')) >=(long)0;

B2 = FILTER B1 BY DaysBetween(dt,(datetime)ToDate('10/01/2015', 'MM/dd/yyyy')) <=(long)0;

C = GROUP B2 ALL;

D = FOREACH C GENERATE  COUNT(B2) ;

DUMP D;
