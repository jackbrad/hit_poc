import json
from langchain.document_loaders import AmazonTextractPDFLoader
from langchain.llms import Bedrock
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
 
 
def lambda_handler(event, context):
    
    bucket = event["Document"]['S3object']['Bucket']
    name = event["Document"]['S3object']['Name']
    file_path = "s3://{bucket}/{name}".format(bucket = bucket, name = name )

    loader = AmazonTextractPDFLoader(file_path)
    document = loader.load()
    
    template = """
    
    Given a full document, give me a summary. 
    
    <document>{doc_text}</document>
    <summary>"""
    
    prompt = PromptTemplate(template=template, input_variables=["doc_text"])
    
    llm = Bedrock(model_id="anthropic.claude-v2", region_name='us-east-1')
    num_tokens = llm.get_num_tokens(document[0].page_content)
    
    llm_chain = LLMChain(prompt=prompt, llm=llm)
    summ = llm_chain.run(document[0].page_content)
    
    #return object
    ret={}
    summary={}
    summary = summ.replace("</summary>","").strip()
    ret['summary'] = summary
    
    print(ret)

    return {
        'statusCode': 200, 
        'body': json.dumps(ret)
    }








