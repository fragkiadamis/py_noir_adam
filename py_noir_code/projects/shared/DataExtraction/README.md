DataExtraction
===

Project used for downloading inputs and outputs of specific processings

# Parameters

### Processing filtering : 

You have to choose between : 
- data = json.dumps(ids) : list of ids in the body of the HTTP request
- "processingComment":"comete_moelle/0.1" : regex corresponding to the processing comment in the headers of the HTTP request

### Output filtering :

You have to chose for the header "resultOnly" one of the choice below :
- "" : no filtering, get all inputs and outputs
- "all" : remove all inputs, get all outputs
- "[any_string]" : remove all inputs, remove all outputs whose name does not contain the string value of the "resultOnly" header