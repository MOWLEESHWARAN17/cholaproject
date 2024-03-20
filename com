import os
from win32com import client as wc

def convert_docx_to_pdf(docx_file, pdf_file):
    # Create an instance of the Word application
    word = wc.Dispatch("Word.Application")

    # Open the input DOCX file
    doc = word.Documents.Open(docx_file)

    # Save as PDF
    doc.SaveAs(pdf_file, FileFormat=17)  # 17 represents PDF format

    # Close the document and Word application
    doc.Close()
    word.Quit()

# Usage example
convert_docx_to_pdf("hindi.docx", "out.pdf")
