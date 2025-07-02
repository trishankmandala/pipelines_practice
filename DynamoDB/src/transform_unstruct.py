import pandas as pd

def transform_data(data):
    project = []
    client = []
    technologies = []
    team_members = []
    milestones = []

    for item in data:
        project_id = item.get('project_id')

        # ---------- Projects Table ----------
        project.append({
            'project_id': project_id,
            'project_name': item.get('project_name'),
            'status': item.get('status'),
            'project_manager': item.get('team', {}).get('project_manager')
        })

        # ---------- Clients Table ----------
        client_data = item.get('client', {})
        location = client_data.get('location', {})
        client.append({
            'project_id': project_id,
            'client_name': client_data.get('name'),
            'client_industry': client_data.get('industry'),
            'client_city': location.get('city'),
            'client_country': location.get('country')
        })

        # ---------- Technologies Table ----------
        for tech in item.get("technologies", []):
            technologies.append({
                "project_id": project_id,
                "technology": tech
            })

        # ---------- Team Members Table ----------
        for member in item.get("team", {}).get("members", []):
            if isinstance(member, dict):
                team_members.append({
                    "project_id": project_id,
                    "name": member.get("name"),
                    "role": member.get("role")
                })

        # ---------- Milestones Table ----------
        for milestone in item.get("milestones", []):
            milestones.append({
                "project_id": project_id,
                "name": milestone.get("name"),
                "due_date": milestone.get("due_date")
            })

    # Convert to DataFrames
    df_projects = pd.DataFrame(project)
    df_clients = pd.DataFrame(client)
    df_technologies = pd.DataFrame(technologies)
    df_team_members = pd.DataFrame(team_members)
    df_milestones = pd.DataFrame(milestones)

    return df_projects, df_clients, df_technologies, df_team_members, df_milestones
