import re
 
def parse_details(text):
 
    #extract name using strip and split ,strip for removing whitespaces and split for first and last name
    lines=text.strip().split('\n')
 
    name=""
    for line in lines:
        if line.strip():
            name=line.strip()
            break
 
   
    #extract email
 
    email_1=re.search(r'[\w\.-]+@[\w\.-]+\.\w+',text)
    email = email_1.group(0) if email_1 else None
 
    #extract phone
    phone_1=re.search(r'(\+91[\s-]?|0)?[6-9]\d{9}', text)
    phone = phone_1.group(0) if phone_1 else None
 
    #extract skills
    skill_keywords = ["skill", "technical skill", "core skill", "key skill"]
    skills = ""
    for i, line in enumerate(lines):
        if any(k in line.lower() for k in skill_keywords):
            skills = "\n".join(lines[i+1:i+5]).strip()
            break
 
 
    #extract projects
    project_keywords = ["project", "academic project", "major project", "internship project"]
    projects = ""
    for i, line in enumerate(lines):
        if any(k in line.lower() for k in project_keywords):
            projects = "\n".join(lines[i+1:i+10]).strip()
            break
 
    return {
        "name": name,
        "email": email,
        "phone": phone,
        "skills": skills,
        "projects": projects
    }
 