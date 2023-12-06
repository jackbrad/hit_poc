from langchain.text_splitter import RecursiveCharacterTextSplitter 
from langchain.embeddings import BedrockEmbeddings
from langchain.vectorstores import FAISS
from langchain.document_loaders import AmazonTextractPDFLoader
from langchain.chains import RetrievalQA


from langchain.document_loaders import AmazonTextractPDFLoader
from langchain.llms import Bedrock
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

from duckpy import Client as DuckyClient

ducky = DuckyClient()


loader = AmazonTextractPDFLoader("25005983-1.pdf")
document = loader.load()

text_splitter = RecursiveCharacterTextSplitter(chunk_size=400,
                                               separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""],
                                               chunk_overlap=0)
texts = text_splitter.split_documents(document)
embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1")
db = FAISS.from_documents(documents=texts,
                           embedding=embeddings) 

retriever = db.as_retriever(search_type='mmr', search_kwargs={"k": 3})

template = """

Answer the question as truthfully as possible strictly using only the provided text, and if the answer is not contained within the text, say "I don't know". Give just the first answer you find.
<text>{context}</text>
<question>{question}</question>
<answer>"""

# define the prompt template
qa_prompt = PromptTemplate(template=template, input_variables=["context","question"])

chain_type_kwargs = { "prompt": qa_prompt, "verbose": False } # change verbose to True if you need to see what's happening
bedrock_llm = Bedrock(model_id="anthropic.claude-v2", region_name='us-east-1')

qa = RetrievalQA.from_chain_type(
    llm=bedrock_llm, 
    chain_type="stuff", 
    retriever=retriever,
    chain_type_kwargs=chain_type_kwargs,
    verbose=False # change verbose to True if you need to see what's happening
)
question="what entities are described in this order?"
result = qa.run(question)
print(result.strip())

ducky_results = ducky.search('who or what is ' + result.strip())
print('ducky results:'  + ducky_results[0].description)