DataAnalysis
===

Project used for downloading some stats on VIP processing filtered on different level of datasets

# Parameters


### Processing filtering :

You can use 3 filters (they can combine) :
- "dataType":dataType : {study, subject, examination, acquisition, dataset} in the header of the HTTP request. If empty, only pipelineIdentifier filter will be taken in account. Read next filter for more details.
- data = json.dumps(ids) : list of ids of the objects relative to the dataType in the body of the HTTP request. Their relative processings will be analysed. If empty, only pipelineIdentifier filter will be taken in account.
- "pipelineIdentifier":"comete_moelle/0.1" : string corresponding to the processing comment in the headers of the HTTP request
