# EC2 Pipeline Deployment Instructions

## Current Status ✅
Your DCIO pipeline has been successfully deployed to AWS EC2 instance:
- **Instance ID:** i-0eaee37f64dfe7195
- **IP Address:** 100.55.101.111
- **Region:** us-east-1
- **Location on EC2:** /home/ec2-user/DCIO-pipeline/DCIO-pipeline
- **Python Version:** 3.11
- **Virtual Environment:** /home/ec2-user/DCIO-pipeline/venv

## Connection Methods

### Option 1: AWS Systems Manager Session Manager (No SSH Keys Needed)
```bash
aws ssm start-session --target i-0eaee37f64dfe7195 --region us-east-1
```

### Option 2: Using AWS CLI to Run Commands Directly
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["cd /home/ec2-user/DCIO-pipeline && source venv/bin/activate && <YOUR_COMMAND>"]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

## Setup Steps

### 1. Configure LLM API Keys (Optional)

#### Option A: Using OpenAI
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "export OPENAI_API_KEY=<YOUR_OPENAI_API_KEY>"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

Replace `<YOUR_OPENAI_API_KEY>` with your actual OpenAI API key.

#### Option B: Using AWS Bedrock (Placeholder for future implementation)
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "export AWS_BEDROCK_MODEL_ID=anthropic.claude-3-sonnet-20240229-v1:0",
    "export AWS_REGION=us-east-1"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

Note: AWS Bedrock support is a placeholder for future implementation.

### 2. Configure AWS Credentials on EC2
Run this command to configure AWS credentials:
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "aws configure set aws_access_key_id YOUR_ACCESS_KEY",
    "aws configure set aws_secret_access_key YOUR_SECRET_KEY",
    "aws configure set region us-east-1",
    "aws configure list"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

Replace `YOUR_ACCESS_KEY` and `YOUR_SECRET_KEY` with your actual AWS credentials.

### 3. Upload Input Files
Copy your PDF files to the EC2 instance:
```bash
# Create a temporary file with your PDFs
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["ls -lh /home/ec2-user/DCIO-pipeline/DCIO-pipeline/data/inputs/"]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

### 4. Configure Environment Variables (Optional)
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "echo \"export USE_LLM=1\" >> /home/ec2-user/.bashrc",
    "echo \"export S3_BUCKET_PATH=s3://retirementinsights-silver/tables/\" >> /home/ec2-user/.bashrc",
    "echo \"export INPUT_DIR=/home/ec2-user/DCIO-pipeline/DCIO-pipeline/data/inputs\" >> /home/ec2-user/.bashrc",
    "echo \"export OUTPUT_DIR=/home/ec2-user/DCIO-pipeline/DCIO-pipeline/data/outputs\" >> /home/ec2-user/.bashrc"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

**Environment Variables:**
- `USE_LLM=1`: Enable LLM-powered data enhancement (default: 1)
- `S3_BUCKET_PATH`: S3 bucket to automatically upload pipeline.db after completion (e.g., `s3://bucket/prefix/`)
- `INPUT_DIR`: Directory containing input PDF files
- `OUTPUT_DIR`: Directory for pipeline output files

### 5. Run the Pipeline

**Basic:**
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "cd /home/ec2-user/DCIO-pipeline/DCIO-pipeline",
    "/home/ec2-user/DCIO-pipeline/venv/bin/python -m src.run_pipeline"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

**With Automatic S3 Upload (Recommended):**
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "export OPENAI_API_KEY=<YOUR_OPENAI_API_KEY>",
    "export S3_BUCKET_PATH=s3://retirementinsights-silver/tables/",
    "cd /home/ec2-user/DCIO-pipeline/DCIO-pipeline",
    "/home/ec2-user/DCIO-pipeline/venv/bin/python -m src.run_pipeline"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

When `S3_BUCKET_PATH` is set, the pipeline automatically uploads `pipeline.db` to S3 after successful completion. ✅

### 6. Check Results
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["ls -lh /home/ec2-user/DCIO-pipeline/DCIO-pipeline/data/outputs/"]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

## Installed Components
- Python 3.11
- Git
- All Python dependencies from requirements.txt:
  - pdfplumber
  - camelot-py
  - ghostscript
  - pdf2image
  - pillow
  - numpy==1.26.4
  - pandas
  - sqlalchemy
  - pydantic
  - python-dotenv
  - openai==1.3.0
  - httpx==0.24.1
  - rapidfuzz
  - pyyaml
  - boto3
  - paddleocr
  - opencv-python

## File Structure on EC2
```
/home/ec2-user/DCIO-pipeline/
├── DCIO-pipeline/              # Main project directory
│   ├── venv/                   # Python 3.11 virtual environment
│   ├── src/                    # Source code
│   │   ├── run_pipeline.py
│   │   ├── ingest.py
│   │   ├── text_extract.py
│   │   ├── llm_map.py
│   │   ├── classify_pages.py
│   │   ├── detect_tables.py
│   │   ├── validate.py
│   │   └── ...
│   ├── config/                 # Configuration files
│   ├── data/
│   │   ├── inputs/            # Your input PDF files go here
│   │   └── outputs/           # Pipeline output files
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── README.md
│   └── ...
├── venv/                       # Shared Python virtual environment (parent level)
├── pipeline/                   # Sample pipeline directory
└── ...
```

## Common Tasks

### View Pipeline Logs
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["tail -f /home/ec2-user/pipeline.log"]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

### Update Code from GitHub
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "cd /home/ec2-user/DCIO-pipeline",
    "git pull"
  ]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

### Check System Resources
```bash
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["free -h", "df -h", "ps aux | grep python"]' \
  --instance-ids "i-0eaee37f64dfe7195" \
  --region us-east-1
```

## Check Command Status
To check the status and output of your commands:
```bash
aws ssm get-command-invocation \
  --command-id <COMMAND_ID> \
  --instance-id i-0eaee37f64dfe7195 \
  --region us-east-1
```

Replace `<COMMAND_ID>` with the command ID returned from any ssm send-command call.

## Next Steps
1. Configure your AWS credentials
2. Upload your PDF input files  
3. Set `S3_BUCKET_PATH` environment variable to enable automatic uploads
4. Run the pipeline with OpenAI API key configured
5. Monitor execution and collect outputs from S3

For more information, see the README.md in the pipeline directory.
