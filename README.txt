mvn clean compile exec:java -Dexec.mainClass="gov.nasa.jpl.cdp.log.App" -Dexec.args="arg0 arg1 arg2"

To call log_parser.java via cdp-services:
(1) cd cdp-services
    mvn clean compile exec:java -Dexec.mainClass="gov.nasa.jpl.cdp.services.Main" -Dexec.args="arg0 arg1 arg2"
(2) curl -v -X POST --data-binary @/home/pan/generateMatchupIndices_provenance.log http://localhost:5000/services/logfile/upload

