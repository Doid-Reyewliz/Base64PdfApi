import base64
import json
from datetime import datetime
from io import BytesIO
import asyncio

from PyPDF2 import PdfReader
from langdetect import detect
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import pandas as pd


app = FastAPI()



async def read_base64(base64_text):
    decoded_bytes = base64.b64decode(base64_text)
    return BytesIO(decoded_bytes)

async def read_pdf_from_bytes(pdf_bytes):
    pdf = PdfReader(pdf_bytes)
    text = ""
    for page in pdf.pages:
        text += page.extract_text()
    return text
    
async def parse_kaspi_statement(text):
    lines = text.split("\n")
    transactions = []
    
    lang = detect(text)
    deposit_sum = 0
    deposit_count = 0
    
    for line in lines:
        parts = line.split("   ")
            
        if len(parts) >= 4:
            transaction_type = parts[1]
            details = parts[3]
            
            if len(parts[0].split('-')) == 2: 
                date, amount = parts[0].split('-')
                amount = "-" + amount
            else: 
                date, amount = parts[0].split('+')
                
            amount_value = int(amount.replace("₸", "").replace(",", "").replace(' ', '').strip())        
            
            transactions.append({
                "amount": amount_value,
                "operationDate": date.strip(),
                "transactionType": transaction_type.strip(),
                "details": details.strip()
            })
            
            if amount_value > 0:  # Only consider deposits
                deposit_sum += amount_value
                deposit_count += 1
    
    avg_sum = int(deposit_sum / deposit_count if deposit_count > 0 else 0)
    


    data = {
        "financialInstitutionName": "Kaspi",
        "cardNumber": text.split("\n")[3].strip().split(" ")[-1].replace("*", ""),
        "fromDate": text.split("\n")[2].strip().split(" ")[-3],
        "toDate": text.split("\n")[2].strip().split(" ")[-1],
        "details": transactions,
        "metrics": {
            "from_date": datetime.strptime(text.split("\n")[2].strip().split(" ")[-3], '%d.%m.%y').strftime('%Y-%m-%dT%H:%M:%S'),
            "to_date": datetime.strptime(text.split("\n")[2].strip().split(" ")[-3], '%d.%m.%y').strftime('%Y-%m-%dT%H:%M:%S'),
            "statement_language": lang,
            "name": text.split("\n")[4].strip().split(" ")[0],
            "surname": text.split("\n")[3].strip().split(" ")[0],
            "patronymic": text.split("\n")[4].strip().split(" ")[1],
            "full_name": text.split("\n")[4].strip().split(" ")[0] + " " + text.split("\n")[3].strip().split(" ")[0] + " " + text.split("\n")[4].strip().split(" ")[1],
            "fin_institut": "Kaspi",
            "card_number": text.split("\n")[3].strip().split(" ")[-1],
            "number_account": text.split("\n")[4].strip().split(" ")[-1],
            "avg_sum": avg_sum
        },
        "statementLanguage": lang,
        "fullName": text.split("\n")[4].strip().split(" ")[0] + " " + text.split("\n")[3].strip().split(" ")[0] + " " + text.split("\n")[4].strip().split(" ")[1],
    }
    
    # print(json.dumps(data, ensure_ascii=False, indent=4))
    
    return data

async def writeToExcel(data):
    # Define the columns and their types
    columns = [
        ("FROM_DATE", "nvarchar(16)"),
        ("TO_DATE", "nvarchar(10)"),
        ("STATEMENT_LANGUAGE", "nvarchar(3)"),
        ("FULL_NAME", "nvarchar(100)"),
        ("FINANSIAL_INSTITUTION", "nvarchar(5)"),
        ("AMOUNT", "float"),
        ("DETAILS", "nvarchar(256)"),
        ("OPERATION_DATE", "nvarchar(256)"),
        ("TRANSACTION_TYPE", "nvarchar(256)"),
        ("INSERT_DATE", "datetime2(7)"),
        ("CARD_NUMBER", "nvarchar(256)"),
        ("ST_CREATION_DATE", "nvarchar(256)"),
        ("ST_MODIFIED_DATE", "nvarchar(256)"),
        ("ST_SUBJECT", "nvarchar(256)"),
        ("ST_AUTHOR", "nvarchar(256)"),
        ("ST_TITLE", "nvarchar(256)"),
        ("ST_PRODUCER", "nvarchar(256)")
    ]

    df = pd.DataFrame(columns=[col[0] for col in columns])

    rows = []
    for transaction in data['details']:
        row = {
            "FROM_DATE": data['metrics']['from_date'],
            "TO_DATE": data['metrics']['to_date'],
            "STATEMENT_LANGUAGE": data['metrics']['statement_language'],
            "FULL_NAME": data['metrics']['full_name'],
            "FINANSIAL_INSTITUTION": data['financialInstitutionName'],
            "AMOUNT": transaction['amount'],
            "DETAILS": transaction['details'],
            "OPERATION_DATE": transaction['operationDate'],
            "TRANSACTION_TYPE": transaction['transactionType'],
            "INSERT_DATE": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            "CARD_NUMBER": data['metrics']['card_number'],
            "ST_CREATION_DATE": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            "ST_MODIFIED_DATE": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            "ST_SUBJECT": "Банковская выписка",
            "ST_AUTHOR": "Система",
            "ST_TITLE": "Банковская выписка за период с " + data['fromDate'] + " по " + data['toDate'],
            "ST_PRODUCER": "Kaspi"
        }
        rows.append(row)

    df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    # df.to_excel(file_path, index=False)
    
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    
    return output


class KaspiStatement(BaseModel):
    base64_pdf: str

@app.post("/{base64_pdf}")      
async def read_path(item: KaspiStatement):
    try:
        pdf_bytes = await read_base64(item.base64_pdf)
        text = await read_pdf_from_bytes(pdf_bytes)
        data = await parse_kaspi_statement(text)
        
        # Excel = await writeToExcel(data)

        response = {
            "success": True,
            "msg": None,
            "msgType": None,
            "data": data
        }
        
        return response
    
    except Exception as e:
        return {
            "success": False,
            "msg": str(e),
            "msgType": "error",
            "data": None
        }



class DatatoExcel(BaseModel):
    data: dict
    
@app.post("/toExcel/{json_data}")
async def read_path(item: DatatoExcel):
    try:
        excel_file = await writeToExcel(item.data)
        
        return StreamingResponse(
            excel_file,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': 'attachment; filename="output.xlsx"'}
        )
    
    except Exception as e:
        return {
            "success": False,
            "msg": str(e),
            "msgType": "error",
            "data": None
        }