import imaplib
import email
import os
import getpass
import zipfile

def connect_to_imap(email_user, email_pass, imap_server):
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(email_user, email_pass)
    return mail

def get_spam_folder(mail):
    spam_folders = ["Spam", "Junk", "[Gmail]/Spam"]
    
    for folder in spam_folders:
        try:
            status, messages = mail.select(folder)
            if status == "OK":
                print(f"Using spam folder: {folder}")
                return folder
        except:
            continue
    
    raise Exception("Spam folder not found. Please enter manually.")

def download_emails(mail, folder, save_dir, max_emails=500):
    os.makedirs(save_dir, exist_ok=True)
    
    mail.select(folder)
    status, messages = mail.search(None, "ALL")
    email_ids = messages[0].split()
    
    count = 0
    for email_id in email_ids[-max_emails:]:
        status, msg_data = mail.fetch(email_id, "(RFC822)")
        
        raw_email = msg_data[0][1]
        
        filename = os.path.join(save_dir, f"email_{count:04d}.eml")
        with open(filename, "wb") as f:
            f.write(raw_email)
        
        count += 1
    
    print(f"Downloaded {count} emails.")

def zip_folder(folder_path, zip_name):
    with zipfile.ZipFile(zip_name, 'w') as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                zipf.write(os.path.join(root, file))

def main():
    email_user = input("Enter your email: ")
    email_pass = getpass.getpass("Enter your app password: ")
    imap_server = input("Enter IMAP server (e.g., imap.gmail.com): ")

    mail = connect_to_imap(email_user, email_pass, imap_server)
    spam_folder = get_spam_folder(mail)
    
    save_dir = f"spam_emails/{email_user}"
    download_emails(mail, spam_folder, save_dir)
    
    zip_name = f"spam_emails_{email_user}.zip"
    zip_folder(save_dir, zip_name)
    
    print(f"Saved zip file: {zip_name}")

if __name__ == "__main__":
    main()