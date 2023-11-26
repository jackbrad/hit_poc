docker build --platform linux/amd64 -t docker-image:brk_sum .

#Set the --region value to the AWS Region where you want to create the Amazon ECR repository.
#Replace 111122223333 with your AWS account ID.
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 111122223333.dkr.ecr.us-east-1.amazonaws.com

#create the repository for the image 
aws ecr create-repository --repository-name brk_sum --region us-east-1 --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE

#get repository Uri from previous output
#{
#    "repository": {
#       .......
#       "repositoryUri": "111122223333.dkr.ecr.us-east-1.amazonaws.com/hello-world",
#       ....... }
#    
#}

#tag container
docker tag docker-image:brk_sum <ECRrepositoryUri>:latest

#upload container to ECR repository
docker push 111122223333.dkr.ecr.us-east-1.amazonaws.com/brk_sum:latest

#make sure you have an execution role for the function
#create the function
aws lambda create-function \
  --function-name brk_sum \
  --package-type Image \
  --code ImageUri=111122223333.dkr.ecr.us-east-1.amazonaws.com/hello-world:latest \
  --role arn:aws:iam::111122223333:role/lambda-ex
