import pandas as pd
 
def transform_dynamic(df: pd.DataFrame):
    if df.empty:
        print("Empty DataFrame.")
        return None
 
    parent_data = []
    members_data = []
    milestones_data = []
    tech_data = []
 
    for row in df.to_dict(orient='records'):
        project_id = row.get('project_id')
 
        # Flatten client and location
        client_info = row.get('client', {})
        client_name = client_info.get('name')
        client_industry = client_info.get('industry')
        location = client_info.get('location', {})
        city = location.get('city')
        country = location.get('country')
 
        team = row.get('team', {})
        manager = team.get('project_manager')
 
        parent_data.append({
            'project_id': project_id,
            'project_name': row.get('project_name'),
            'client_name': client_name,
            'client_industry': client_industry,
            'client_city': city,
            'client_country': country,
            'status': row.get('status'),
            'project_manager': manager
        })
 
        # Extract team members
        for member in team.get('members', []):
            members_data.append({
                'project_id': project_id,
                'name': member.get('name'),
                'role': member.get('role')
            })
 
        # Extract milestones
        for milestone in row.get('milestones', []):
            milestones_data.append({
                'project_id': project_id,
                'milestone_name': milestone.get('name'),
                'due_date': milestone.get('due_date')
            })
 
        # Extract technologies
        for tech in row.get('technologies', []):
            tech_data.append({
                'project_id': project_id,
                'technology': tech
            })
 
    df_project = pd.DataFrame(parent_data)
    df_members = pd.DataFrame(members_data)
    df_milestones = pd.DataFrame(milestones_data)
    df_technologies = pd.DataFrame(tech_data)
 
    return {
        'projects': df_project,
        'team_members': df_members,
        'milestones': df_milestones,
        'technologies': df_technologies
    }
 