from langchain.document_loaders import AmazonTextractPDFLoader
from langchain.llms import Bedrock
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import json

def handler(event, context): 

    bucket = event["Document"]['S3Object']['Bucket']
    name = event["Document"]['S3Object']['Name']
    file_path = "s3://{bucket}/{name}".format(bucket = bucket, name = name )

    loader = AmazonTextractPDFLoader(file_path)
    document = loader.load()

    bedrock_llm = Bedrock(model_id="anthropic.claude-v2", region_name='us-east-1')

    template1 = """

    Given a full document, answer the question and format the output in the format specified. Skip any preamble text and just generate the JSON.

    <format>
    {{
    "key_name":"key_value"
    }}
    </format>
    <document>{doc_text}</document>
    <question>{question}</question>"""

    template2 = """

    Given a JSON document, format the dates in the value fields precisely in the provided format. Skip any preamble text and just generate the JSON.

    <format>DD/MM/YYYY</format>
    <json_document>{json_doc}</json_document>
    """


    prompt1 = PromptTemplate(template=template1, input_variables=["doc_text", "question"])
    llm_chain = LLMChain(prompt=prompt1, llm=bedrock_llm, verbose=True)

    prompt2 = PromptTemplate(template=template2, input_variables=["json_doc"])
    llm_chain2 = LLMChain(prompt=prompt2, llm=bedrock_llm, verbose=True)

    chain = ( 
        llm_chain 
        | {'json_doc': lambda x: x['text'] }  
        | llm_chain2
    )

    std_op = chain.invoke({ "doc_text": document[0].page_content, 
                            "question": "Can you give me order date and order number"})

    #return object
    ret={}
    schematized={}
    schematized = std_op
    ret['schematized'] = schematized
    
    print(ret)

    return {
        'statusCode': 200, 
        'body': ret
    }
