DataAnalysis
===

Project used for downloading some stats on VIP processing filtered on different level of datasets

# Parameters


### Processing filtering :

You can use 3 filters (they can combine) :
- "dataType":dataType : {study, subject, examination, acquisition, dataset} in the header of the HTTP request. Read next filter for more details.
- data = json.dumps(ids) : list of ids of the objects relative to the dataType in the body of the HTTP request. Their relative processings will be analysed.
- "pipelineIdentifier":"comete_moelle/0.1" : string corresponding to the processing comment in the headers of the HTTP request

Care, at least one among pipelineIdentifier or <dataType, ids> pair needs to be defined, otherwise an HTTP error is returned