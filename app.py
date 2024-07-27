from flask import Flask, request, jsonify
import os
import ast
import re
import pdfplumber
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB credentials
mongo_uri = "your_mongo_uri"
database_name = "MAXLIFE"
collection_name = "MAXLIFE_DATA"

client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# Define a folder for uploads
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def sanitize_keys(document):
    if isinstance(document, dict):
        return {k.replace('.', '_'): sanitize_keys(v) for k, v in document.items()}
    elif isinstance(document, list):
        return [sanitize_keys(item) for item in document]
    else:
        return document

def extract_table_from_first_page(pdf_path, output_txt_path):
    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) > 0:
            first_page = pdf.pages[5]
            tables = first_page.extract_tables()
            if tables:
                with open(output_txt_path, 'w', encoding='utf-8') as text_file:
                    for row in tables:
                        cleaned_row = [str(cell).replace('\n', ' ').replace('\r', '') for cell in row]
                        text_file.write('|'.join(cleaned_row) + '\n')
                print(f"Table from the first page has been saved to {output_txt_path}")
            else:
                print("No table found on the first page.")
        else:
            print("The PDF has no pages.")

def extract_text_from_first_page(pdf_path, output_txt_path):
    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) > 0:
            first_page = pdf.pages[2]
            text = first_page.extract_text()
            if text:
                with open(output_txt_path, 'w', encoding='utf-8') as text_file:
                    text_file.write(text)
                print(f"Text from the first page has been saved to {output_txt_path}")
            else:
                print("No text found on the first page.")
        else:
            print("The PDF has no pages.")

def process_pdf(pdf_path, executive_id, executive_name):
    table_txt_path = 'first_page_table.txt'
    text_txt_path = 'txt_file.txt'
    extract_table_from_first_page(pdf_path, table_txt_path)
    extract_text_from_first_page(pdf_path, text_txt_path)
    header = []
    data = []
    with open(text_txt_path, 'r') as file:
        content = file.read()
        policyholder_name = re.search(r'Policyholder/ Life Insured (.*?) Premium Payment Mode', content).group(1).strip()
        premium_payment_mode = re.search(r'Premium Payment Mode: (.*)', content).group(1).strip()
        sum_assured = re.search(r'Sum Assured: `([\d,]+\.?\d*)', content).group(1).strip()
        policy_number = re.search(r'Policy No\.: (\d+)', content).group(1).strip()
        commencement_date = re.search(r'Date of Commencement: ([\d\w-]+)', content).group(1).strip()
        monthly_income_benefit = re.search(r'Monthly Income Benefit: ([\d,\.]+)', content).group(1).strip()
        maturity_date = re.search(r'Maturity Date: ([\d\w-]+)', content).group(1).strip()
        policy_term = re.search(r'Policy Term \(in years\): (\d+)', content).group(1).strip()
        death_benefit = re.search(r'Death Benefit: `([\d,\.]+)', content).group(1).strip()
        premium_payment_term = re.search(r'Premium Payment Term \(in (\d+)', content).group(1).strip()
        premium_amount = re.search(r'Premium Amount `([\d,\.]+)', content).group(1).strip()
        premium_payment_due_date = re.search(r'Premium Payment Due Date: (.*)', content).group(1).strip()
        last_premium_due_date = re.search(r'Last Premium Due Date: ([\d\w-]+)', content).group(1).strip()
        attributes = [
            ("Policyholder Name", policyholder_name),
            ("Premium Payment Mode", premium_payment_mode),
            ("Policy Number", policy_number),
            ("Date of Commencement", commencement_date),
            ("Sum Assured", sum_assured),
            ("Monthly Income Benefit", monthly_income_benefit),
            ("Maturity Date", maturity_date),
            ("Policy Term", policy_term),
            ("Death Benefit", death_benefit),
            ("Premium Payment Term", premium_payment_term),
            ("Premium Amount", premium_amount),
            ("Premium Payment Due Date", premium_payment_due_date),
            ("Last Premium Due Date", last_premium_due_date),
        ]
        for h, value in attributes:
            header.append(h)
            data.append(value)
    with open(table_txt_path, 'r') as file:
        table = file.read().strip().rstrip('\n')
        table = table.split("|")
        for i in range(len(table)):
            table[i] = ast.literal_eval(table[i])
        text = table[0][0]
        policy_no = re.search(r'Policy No./ Proposal No\.:(.*)', text).group(1).strip() if re.search(r'Policy No./ Proposal No\.:(.*)', text) else ""
        date_of_proposal = re.search(r'Date of Proposal:(.*)', text).group(1).strip() if re.search(r'Date of Proposal:(.*)', text) else ""
        header.append("Policy No_/ Proposal No")
        header.append("Date of Proposal")
        data.extend([policy_no, date_of_proposal])
        text = table[0][1]
        client_id = re.search(r'Client ID:\s*(.*)', text).group(1).strip() if re.search(r'Client ID:\s*(.*)', text) else ""
        text = table[1][0]
        policyholder = re.search(r'Policyholder/Proposer :\s*(.*)', text).group(1).strip() if re.search(r'Policyholder/Proposer :\s*(.*)', text) else ""
        pan = re.search(r'PAN:\s*(.*)', text).group(1).strip() if re.search(r'PAN:\s*(.*)', text) else ""
        relationship = re.search(r'Relationship with Life Insured:\s*(.*)', text).group(1).strip() if re.search(r'Relationship with Life Insured:\s*(.*)', text) else ""
        date_of_birth = re.search(r'Date of Birth:\s*(.*)', text).group(1).strip() if re.search(r'Date of Birth:\s*(.*)', text) else ""
        address = re.search(r'Address:(.*?)(?=Date of Birth:|$)', text, re.DOTALL).group(1).strip() if re.search(r'Address:(.*?)(?=Date of Birth:|$)', text, re.DOTALL) else ""
        header.extend(["policyholder", "pan", "relationship", "date_of_birth", "address"])
        data.extend([policyholder, pan, relationship, date_of_birth, address])
        text = table[1][1]
        age_admitted = re.search(r'Age Admitted:\s*(.*)', text).group(1).strip() if re.search(r'Age Admitted:\s*(.*)', text) else ""
        gender = re.search(r'Gender:\s*(.*)', text).group(1).strip() if re.search(r'Gender:\s*(.*)', text) else ""
        tel_no = re.search(r'Tel No./Mobile No.:\s*(.*)', text).group(1).strip() if re.search(r'Tel No./Mobile No.:\s*(.*)', text) else ""
        email = re.search(r'Email:\s*(.*)', text).group(1).strip() if re.search(r'Email:\s*(.*)', text) else ""
        header.extend(["gender", "tel_no", "email"])
        data.extend([gender, tel_no, email])
        text = table[2][0]
        life_insured = re.search(r'Life Insured:\s*(.*)', text).group(1).strip() if re.search(r'Life Insured:\s*(.*)', text) else ""
        date_of_birth = re.search(r'Date of Birth:\s*(.*)', text).group(1).strip() if re.search(r'Date of Birth:\s*(.*)', text) else ""
        age = re.search(r'Age:\s*(.*)', text).group(1).strip() if re.search(r'Age:\s*(.*)', text) else ""
        text = table[2][1]
        age_admitted = re.search(r'Age Admitted:\s*(.*)', text).group(1).strip() if re.search(r'Age Admitted:\s*(.*)', text) else ""
        gender = re.search(r'Gender:\s*(.*)', text).group(1).strip() if re.search(r'Gender:\s*(.*)', text) else ""
        text = table[3][0]
        nominee = re.search(r'Nominee\(s\):\s*(.*)', text).group(1).strip() if re.search(r'Nominee\(s\):\s*(.*)', text) else ""
        header.extend(["life_insured", "date_of_birth", "age", "age_admitted", "nominee", "Executive ID", "Executive Name"])
        data.extend([life_insured, date_of_birth, age, age_admitted, nominee, executive_id, executive_name])
    document = dict(zip(header, data))
    sanitized_document = sanitize_keys(document)
    collection.insert_one(sanitized_document)
    print("Data has been inserted into MongoDB.")
    os.remove(pdf_path)
    os.remove(table_txt_path)
    os.remove(text_txt_path)
    print("Files have been deleted.")

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    executive_id = request.form.get('executive_id')
    executive_name = request.form.get('executive_name')
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if not executive_id or not executive_name:
        return jsonify({"error": "Executive ID and Executive Name are required"}), 400
    if file and file.filename.endswith('.pdf'):
        pdf_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(pdf_path)
        process_pdf(pdf_path, executive_id, executive_name)
        return jsonify({"message": "File processed successfully"}), 200
    else:
        return jsonify({"error": "Invalid file type"}), 400
