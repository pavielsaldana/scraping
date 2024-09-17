import base64
import json
import numpy as np
import os
import pandas as pd
import random
import requests
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
import time
import uuid
from datetime import datetime, timezone
from io import StringIO
from tqdm import tqdm

def linkedin_outreach_scripts(
    result_column_name: str = "Done?",
    csrf_token: str = None, 
    dataframe: pd.DataFrame = None, 
    script_type: str = None, 
    conversation_id_column_name: str = None, 
    waiting_time_min: int = None, 
    waiting_time_max: int = None, 
    message_column_name: str = None, 
    vmid_column_name: str = None, 
    action: str = None, 
    cookies_dict: dict = None, 
    invitation_id_column_name: str = None, 
    invitation_shared_secret_column_name: str = None, 
    unique_identifier_column_name: str = None,
    streamlit_execution: bool = False
) -> None:
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    #Functional functions
    def convertToTimestamp(milliseconds):
        if milliseconds:
            return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None
    def get_conversations():
        params = {"keyVersion": "LEGACY_INBOX"}
        request_url = "https://www.linkedin.com/voyager/api/messaging/conversations"
        response = requests.get(url=request_url, params=params,
                                cookies=cookies_dict, headers=headers)
        return response.json()
    def get_conversation(conversation_id):
        # -->Get a conversation passing conversation id
        request_url = f'https://www.linkedin.com/voyager/api/messaging/conversations/{conversation_id}/events'
        response = requests.get(
            url=request_url, cookies=cookies_dict, headers=headers)
        return response.json()
    def get_conversation_id_using_vmid(vmid):
        # -->Helper function
        # -->Get id of a conversation passing person vmid
        request_url = f"https://www.linkedin.com/voyager/api/messaging/conversations?\
        keyVersion=LEGACY_INBOX&q=participants&recipients=List({vmid})"
        response = requests.get(
            url=request_url, cookies=cookies_dict, headers=headers)
        data = response.json()
        if safe_extract(data, "elements") == []:
            return None
        dashEntityUrn = safe_extract(data, "elements", 0, "dashEntityUrn")
        try:
            dashEntityUrn = dashEntityUrn.split(':')[-1]
        except:
            pass
        return dashEntityUrn
    def get_user_profile():
        # -->Get data from current user
        request_url = "https://www.linkedin.com/voyager/api/me"
        response = requests.get(
            url=request_url, cookies=cookies_dict, headers=headers)
        data = response.json()
        return data
    def generate_trackingId_as_charString():
        # -->Helper function
        random_int_array = [random.randrange(256) for _ in range(16)]
        rand_byte_array = bytearray(random_int_array)
        return "".join([chr(i) for i in rand_byte_array])
    def send_message(message_body, conversation_id, is_premium):
        # -->Send message passing a conversation id
        params = {"action": "create"}
        if not conversation_id:
            return print("Must provide conversation id!")
        if is_premium == True and len(message_body) > 300:
            return print("The message must be less than 300 characters!")
        if is_premium == False and len(message_body) > 200:
            return print("The message must be less than 200 characters!")
        message_event = {
            "eventCreate": {
                "originToken": str(uuid.uuid4()),
                "value": {
                    "com.linkedin.voyager.messaging.create.MessageCreate": {
                        "attributedBody": {
                            "text": message_body,
                            "attributes": [],
                        },
                        "attachments": [],
                    }
                },
                "trackingId": generate_trackingId_as_charString(),
            },
            "dedupeByClientGeneratedToken": False,
        }
        if conversation_id:
            request_url = f"https://www.linkedin.com/voyager/api/messaging/conversations/{conversation_id}/events"
            response = requests.post(url=request_url, params=params, data=json.dumps(
                message_event), headers=headers, cookies=cookies_dict)
            if response.status_code == 201:
                return print("Message sent!")
            else:
                return print("Message not sent!")
    def mark_conversation_as_seen(conversation_id):
        # -->Mark conversation as seen using a conversation id
        payload = json.dumps({"patch": {"$set": {"read": True}}})
        request_url = f"https://www.linkedin.com/voyager/api/messaging/conversations/{conversation_id}"
        response = requests.post(
            url=request_url, data=payload, headers=headers, cookies=cookies_dict)
        if response.status_code == 200:
            return print("Conversation marked as seen!")
        else:
            return print("Conversation not marked as seen!")
    def get_all_invitations():
        # -->Get all current connection requests
        invitations = []
        start = 0
        count = 100
        has_more = True
        params = {"start": start, "count": count,
                "includeInsights": True, "q": "receivedInvitation"}
        while has_more:
            request_url = "https://www.linkedin.com/voyager/api/relationships/invitationViews"
            response = requests.get(
                url=request_url, params=params, cookies=cookies_dict, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch invitations: {response.status_code}")
                break
            response_payload = response.json()
            current_invitations = [
                element for element in response_payload.get("elements", [])]
            if not current_invitations:
                has_more = False
            else:
                invitations.extend(current_invitations)
                params['start'] += count
        return invitations
    def reply_invitation(invitation_id, invitation_shared_secret, action, isGenericInvitation=True):
        # -->Accept a invitation using invitation id and invitation shared secret
        # action: accept or ignore
        params = {"action": action}
        payload = json.dumps({"invitationId": invitation_id, "invitationSharedSecret":
                            invitation_shared_secret, "isGenericInvitation": isGenericInvitation})
        request_url = f"https://www.linkedin.com/voyager/api/relationships/invitations/{invitation_id}"
        response = requests.post(url=request_url, params=params,
                                data=payload, cookies=cookies_dict, headers=headers)
        if action == "accept":
            if response.status_code == 200:
                return print(f"Invitation accepted!")
            else:
                return print(f"Invitation not accepted!")
        elif action == "ignore":
            if response.status_code == 200:
                return print(f"Invitation ignored!")
            else:
                return print(f"Invitation not ignored!")
    def generate_trackingId():
        # -->Helper function
        random_int_array = [random.randrange(256) for _ in range(16)]
        rand_byte_array = bytearray(random_int_array)
        return base64.b64encode(rand_byte_array).decode('utf-8')
    def add_connection(uniqueIdentifier, message=""):
        # -->Send connection request passing its vmid or universal name
        if len(message) > 300:
            return print("Message too long. Max size is 300 characters")
        trackingId = generate_trackingId()
        payload = {"trackingId": trackingId, "message": message, "invitations": [], "excludeInvitations": [
        ], "invitee": {"com.linkedin.voyager.growth.invitation.InviteeProfile": {"profileId": uniqueIdentifier}}}
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        request_url = "https://www.linkedin.com/voyager/api/growth/normInvitations"
        response = requests.post(url=request_url, data=json.dumps(
            payload), headers=headers, cookies=cookies_dict)
        if 'accept' in headers:
            del headers['accept']
        if response.status_code == 201:
            return print("Connection request sent!")
        else:
            return print("Connection request not sent!")
    def remove_connection(uniqueIdentifier):
        # -->Remove connection using vmid or universal name
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        request_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{uniqueIdentifier}/profileActions?action=disconnect"
        response = requests.post(
            url=request_url, headers=headers, cookies=cookies_dict)
        if 'accept' in headers:
            del headers['accept']
        if response.status_code == 200:
            return print("Connection removed!")
        else:
            return print("Connection not removed!")
    def follow_unfollow_profile(uniqueIdentifier, action):
        # -->Follow or unfollow using vmid
        if action == "unfollow":
            action_variable = "unfollowByEntityUrn"
        elif action == "follow":
            action_variable = "followByEntityUrn"
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        payload = {"urn": f"urn:li:fs_followingInfo:{uniqueIdentifier}"}
        request_url = f"https://www.linkedin.com/voyager/api/feed/follows?action={action_variable}"
        response = requests.post(url=request_url, headers=headers, data=json.dumps(
            payload), cookies=cookies_dict)
        if 'accept' in headers:
            del headers['accept']
        if action == "unfollow":
            if response.status_code == 200:
                return print("Unfollowed!")
            else:
                return print(f"Not unfollowed!")
        elif action == "follow":
            if response.status_code == 200:
                return print("Followed!")
            else:
                return print("Not followed!")
    def get_all_connections():
        # -->Get all current connections
        connections = []
        start = 0
        count = 100
        has_more = True
        params = {"decorationId": "com.linkedin.voyager.dash.deco.web.mynetwork.ConnectionListWithProfile-16",
                "start": start, "count": count, "q": "search", "sortType": "RECENTLY_ADDED"}
        while has_more:
            request_url = 'https://www.linkedin.com/voyager/api/relationships/dash/connections'
            response = requests.get(
                url=request_url, params=params, cookies=cookies_dict, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch connections: {response.status_code}")
                break
            response_payload = response.json()
            current_connections = [
                element for element in response_payload.get("elements", [])]
            if not current_connections:
                has_more = False
            else:
                connections.extend(current_connections)
                params['start'] += count
        return connections
    def get_all_sent_invitations():
        # -->Get all current sent connection requests
        invitations = []
        start = 0
        count = 100
        has_more = True
        while has_more:
            request_url = f"https://www.linkedin.com/voyager/api/graphql?variables=(start:{start},count:{count},invitationType:CONNECTION)&queryId=voyagerRelationshipsDashSentInvitationViews.ae305855593fe45f647a54949e3b3c96"
            response = requests.get(
                url=request_url, cookies=cookies_dict, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch sent invitations: {response.status_code}")
                break
            response_payload = response.json()
            current_invitations = [element for element in safe_extract(
                response_payload, "data", "relationshipsDashSentInvitationViewsByInvitationType", "elements")]
            if not current_invitations:
                has_more = False
            else:
                invitations.extend(current_invitations)
                start += count
        return invitations
    def withdraw_invitation(invitation_id):
        # -->Withdraw a invitation using invitation id
        params = {"action": "withdraw"}
        payload = json.dumps({"invitationId": invitation_id})
        request_url = f"https://www.linkedin.com/voyager/api/relationships/invitations/{invitation_id}"
        response = requests.post(url=request_url, params=params,
                                data=payload, cookies=cookies_dict, headers=headers)
        if response.status_code == 200:
            return print(f"Invitation withdrawed!")
        else:
            return print(f"Invitation not withdrawed!")
    #Using functions
    def get_last_20_conversations():
        # -->get_conversations
        all_conversations_json = get_conversations()
        all_conversations = safe_extract(all_conversations_json, "elements")
        df_all_conversations_final = pd.DataFrame()
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_last_20_conversations = st.progress(0)
            number_iterations = len(all_conversations)
            index = 0
        #--STREAMLIT--#
        for conversation in all_conversations:
            df_all_conversations_loop = pd.DataFrame()
            conversation_dashEntityUrn = safe_extract(conversation, "dashEntityUrn")
            try:
                conversation_dashEntityUrn = conversation_dashEntityUrn.split(':')[-1]
            except:
                pass
            # Conversation
            conversation_inboxType = safe_extract(conversation, "inboxType")
            conversation_unreadCount = safe_extract(conversation, "unreadCount")
            conversation_lastActivityAt = convertToTimestamp(safe_extract(conversation, "lastActivityAt"))
            conversation_lastReadAt = convertToTimestamp(safe_extract(conversation, "lastReadAt"))
            conversation_archived = safe_extract(conversation, "archived")
            conversation_blocked = safe_extract(conversation, "blocked")
            conversation_starred = safe_extract(conversation, "starred")
            conversation_withNonConnection = safe_extract(conversation, "withNonConnection")
            conversation_muted = safe_extract(conversation, "muted")
            # Latest message
            latest_createdAt = convertToTimestamp(safe_extract(conversation, "events", 0, "createdAt"))
            latest_eventContent = safe_extract(conversation, "events", 0, "eventContent", "com.linkedin.voyager.messaging.event.MessageEvent", "attributedBody", "text")
            latest_firstName = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "firstName")
            latest_lastName = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "lastName")
            latest_dashEntityUrn = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "dashEntityUrn")
            try:
                latest_dashEntityUrn = latest_dashEntityUrn.split(':')[-1]
            except:
                pass
            latest_standardizedPronoun = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "standardizedPronoun")
            latest_occupation = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "occupation")
            latest_objectUrn = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "objectUrn")
            try:
                latest_objectUrn = latest_objectUrn.split(':')[-1]
            except:
                pass
            latest_background_artifacts = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
            latest_background_rootUrl = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
            latest_banner200x800 = latest_banner350x1400 = None
            if latest_background_artifacts and latest_background_rootUrl:
                for latest_artifact in latest_background_artifacts:
                    latest_file_segment = latest_artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in latest_file_segment:
                        latest_banner200x800 = f"{latest_background_rootUrl}{latest_file_segment}"
                    elif '350_1400' in latest_file_segment:
                        latest_banner350x1400 = f"{latest_background_rootUrl}{latest_file_segment}"
                    if latest_banner200x800 and latest_banner350x1400:
                        break
            latest_publicIdentifier = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
            latest_picture_artifacts = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
            latest_picture_rootUrl = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
            latest_picture100x100 = latest_picture200x200 = latest_picture400x400 = latest_picture800x800 = None
            if latest_picture_artifacts and latest_picture_rootUrl:
                for latest_artifact in latest_picture_artifacts:
                    latest_file_segment = latest_artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in latest_file_segment:
                        latest_picture100x100 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    elif '200_200' in latest_file_segment:
                        latest_picture200x200 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    elif '400_400' in latest_file_segment:
                        latest_picture400x400 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    elif '800_800' in latest_file_segment:
                        latest_picture800x800 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    if latest_picture100x100 and latest_picture200x200 and latest_picture400x400 and latest_picture800x800:
                        break
            latest_nameInitials = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "nameInitials")
            # Person
            firstName = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "firstName")
            lastName = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "lastName")
            dashEntityUrn = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "dashEntityUrn")
            try:
                dashEntityUrn = dashEntityUrn.split(':')[-1]
            except:
                pass
            standardizedPronoun = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "standardizedPronoun")
            occupation = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "occupation")
            objectUrn = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "objectUrn")
            try:
                objectUrn = objectUrn.split(':')[-1]
            except:
                pass
            background_artifacts = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
            background_rootUrl = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
            banner200x800 = banner350x1400 = None
            if background_artifacts and background_rootUrl:
                for artifact in background_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in file_segment:
                        banner200x800 = f"{background_rootUrl}{file_segment}"
                    elif '350_1400' in file_segment:
                        banner350x1400 = f"{background_rootUrl}{file_segment}"
                    if banner200x800 and banner350x1400:
                        break
            publicIdentifier = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
            picture_artifacts = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
            picture_rootUrl = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            nameInitials = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "nameInitials")
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["conversation_dashEntityUrn", "conversation_inboxType", "conversation_unreadCount", "conversation_lastActivityAt", "conversation_lastReadAt", "conversation_archived", "conversation_blocked", "conversation_starred", "conversation_withNonConnection", "conversation_muted", "latest_createdAt", "latest_eventContent", "latest_firstName", "latest_lastName", "latest_dashEntityUrn", "latest_standardizedPronoun", "latest_occupation", "latest_objectUrn", "latest_banner200x800", "latest_banner350x1400", "latest_publicIdentifier", "latest_picture100x100", "latest_picture200x200", "latest_picture400x400", "latest_picture800x800", "latest_nameInitials", "firstName", "lastName", "dashEntityUrn", "standardizedPronoun", "occupation", "objectUrn", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "nameInitials"]}
            df_all_conversations_loop = pd.DataFrame(selected_vars)
            df_all_conversations_final = pd.concat(
                [df_all_conversations_final, df_all_conversations_loop])
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_get_last_20_conversations.progress(index / number_iterations)
            #--STREAMLIT--#
        all_conversations_rename_dict = {
            "conversation_dashEntityUrn": "Conversation - ID",
            "conversation_inboxType": "Conversation - Inbox type",
            "conversation_unreadCount": "Conversation - Unread messages count",
            "conversation_lastActivityAt": "Conversation - Last activity",
            "conversation_lastReadAt": "Conversation - Last read",
            "conversation_archived": "Conversation - Archived?",
            "conversation_blocked": "Conversation - Blocked?",
            "conversation_starred": "Conversation - Starred?",
            "conversation_withNonConnection": "Conversation - With non connection?",
            "conversation_muted": "Conversation - Muted?",
            "latest_createdAt": "Latest message - Creation time",
            "latest_eventContent": "Latest message - Text",
            "latest_firstName": "Latest message - Person first name",
            "latest_lastName": "Latest message - Person last name",
            "latest_dashEntityUrn": "Latest message - Person vmid",
            "latest_standardizedPronoun": "Latest message - Person pronoun",
            "latest_occupation": "Latest message - Occupation",
            "latest_objectUrn": "Latest message - Person user ID",
            "latest_banner200x800": "Latest message - Person banner 200x800",
            "latest_banner350x1400": "Latest message - Person banner 350x1400",
            "latest_publicIdentifier": "Latest message - Person universal name",
            "latest_picture100x100": "Latest message - Person picture 100x100",
            "latest_picture200x200": "Latest message - Person banner 200x200",
            "latest_picture400x400": "Latest message - Person picture 400x400",
            "latest_picture800x800": "Latest message - Person picture 800x800",
            "latest_nameInitials": "Latest message - Person name initials",
            "firstName": "Person - First name",
            "lastName": "Person - Last name",
            "dashEntityUrn": "Person - vmid",
            "standardizedPronoun": "Person - Pronoun",
            "occupation": "Person - Occupation",
            "objectUrn": "Person - User ID",
            "banner200x800": "Person - Banner 200x800",
            "banner350x1400": "Person - Banner 350x1400",
            "publicIdentifier": "Person - Universal name",
            "picture100x100": "Person - Picture 100x100",
            "picture200x200": "Person - Picture 200x200",
            "picture400x400": "Person - Picture 400x400",
            "picture800x800": "Person - Picture 800x800",
            "nameInitials": "Person - Name initials"
        }
        df_all_conversations_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return df_all_conversations_final
    def get_all_messages_from_conversation(dataframe, conversation_id_column_name):
        dataframe.drop_duplicates(subset=[conversation_id_column_name], inplace=True)
        columnName_values = dataframe[conversation_id_column_name].tolist()
        df_all_messages_final = pd.DataFrame()
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_all_messages_from_conversation = st.progress(0)
            number_iterations = len(columnName_values)
            index = 0
        #--STREAMLIT--#
        for item in columnName_values:
            all_messages_json = get_conversation(item)
            all_messages = safe_extract(all_messages_json, "elements")
            for message in all_messages:
                df_all_messages_loop = pd.DataFrame()
                conversation_dashEntityUrn = item
                createdAt = convertToTimestamp(safe_extract(message, "createdAt"))
                eventContent = safe_extract(message, "eventContent", "com.linkedin.voyager.messaging.event.MessageEvent", "attributedBody", "text")
                firstName = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "firstName")
                lastName = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "lastName")
                dashEntityUrn = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "dashEntityUrn")
                try:
                    dashEntityUrn = dashEntityUrn.split(':')[-1]
                except:
                    pass
                occupation = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "occupation")
                objectUrn = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "objectUrn")
                try:
                    objectUrn = objectUrn.split(':')[-1]
                except:
                    pass
                background_artifacts = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
                background_rootUrl = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
                banner200x800 = banner350x1400 = None
                if background_artifacts and background_rootUrl:
                    for artifact in background_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '200_800' in file_segment:
                            banner200x800 = f"{background_rootUrl}{file_segment}"
                        elif '350_1400' in file_segment:
                            banner350x1400 = f"{background_rootUrl}{file_segment}"
                        if banner200x800 and banner350x1400:
                            break
                publicIdentifier = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
                picture_artifacts = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
                picture_rootUrl = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
                publicIdentifier = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
                nameInitials = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "nameInitials")
                all_variables = locals()
                selected_vars = {var: [all_variables[var]] for var in ["conversation_dashEntityUrn", "createdAt", "eventContent", "firstName", "lastName", "dashEntityUrn", "occupation", "objectUrn", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "publicIdentifier", "nameInitials"]}
                df_all_messages_loop = pd.DataFrame(selected_vars)
                df_all_messages_final = pd.concat(
                    [df_all_messages_final, df_all_messages_loop])
                #--STREAMLIT--#
                if streamlit_execution:
                    index += 1
                    progress_bar_get_all_messages_from_conversation.progress(index / number_iterations)
                #--STREAMLIT--#
        df_all_messages_final['createdAt'] = pd.to_datetime(df_all_messages_final['createdAt'])
        df_all_messages_final.sort_values(by=['conversation_dashEntityUrn', 'createdAt'], inplace=True)
        all_messages_rename_dict = {
            "conversation_dashEntityUrn": "Conversation - ID",
            "createdAt": "Message - Creation time",
            "eventContent": "Message - Text",
            "firstName": "Person - First name",
            "lastName": "Person - Last name",
            "dashEntityUrn": "Person - vmid",
            "occupation": "Person - Occupation",
            "objectUrn": "Person - User ID",
            "banner200x800": "Person - Banner 200x800",
            "banner350x1400": "Person - Banner 350x1400",
            "publicIdentifier": "Person - Universal name",
            "picture100x100": "Person - Picture 100x100",
            "picture200x200": "Person - Picture 200x200",
            "picture400x400": "Person - Picture 400x400",
            "picture800x800": "Person - Picture 800x800",
            "nameInitials": "Person - Name initials"
        }
        df_all_messages_final.rename(columns=all_messages_rename_dict, inplace=True)
        return df_all_messages_final    
    def obtain_current_user_profile():
        #-->get_user_profile
        profile_data = get_user_profile()
        plainId = safe_extract(profile_data, "plainId")
        firstName = safe_extract(profile_data, "miniProfile", "firstName")
        lastName = safe_extract(profile_data, "miniProfile", "lastName")
        dashEntityUrn = safe_extract(profile_data, "miniProfile", "dashEntityUrn")
        try:
            dashEntityUrn = dashEntityUrn.split(':')[-1]
        except:
            pass
        occupation = safe_extract(profile_data, "miniProfile", "dashEntityUrn")
        background_artifacts = safe_extract(profile_data, "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
        background_rootUrl = safe_extract(profile_data, "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
        banner200x800 = banner350x1400 = None
        if background_artifacts and background_rootUrl:
            for artifact in background_artifacts:
                file_segment = artifact['fileIdentifyingUrlPathSegment']
                if '200_800' in file_segment:
                    banner200x800 = f"{background_rootUrl}{file_segment}"
                elif '350_1400' in file_segment:
                    banner350x1400 = f"{background_rootUrl}{file_segment}"
                if banner200x800 and banner350x1400:
                    break
        publicIdentifier = safe_extract(profile_data, "miniProfile", "publicIdentifier")
        picture_artifacts = safe_extract(profile_data, "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
        picture_rootUrl = safe_extract(profile_data, "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
        picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
        if picture_artifacts and picture_rootUrl:
            for artifact in picture_artifacts:
                file_segment = artifact['fileIdentifyingUrlPathSegment']
                if '100_100' in file_segment:
                    picture100x100 = f"{picture_rootUrl}{file_segment}"
                elif '200_200' in file_segment:
                    picture200x200 = f"{picture_rootUrl}{file_segment}"
                elif '400_400' in file_segment:
                    picture400x400 = f"{picture_rootUrl}{file_segment}"
                elif '800_800' in file_segment:
                    picture800x800 = f"{picture_rootUrl}{file_segment}"
                if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                    break
        premiumSubscriber = safe_extract(profile_data, "premiumSubscriber")
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["plainId", "firstName", "lastName", "dashEntityUrn", "occupation", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400","picture800x800", "premiumSubscriber"]}
        df_profile_data = pd.DataFrame(selected_vars)
        profile_data_rename_dict = {
            "plainId": "User ID",
            "firstName": "First name",
            "lastName": "Last name",
            "dashEntityUrn": "vmid",
            "occupation": "Occupation",
            "banner200x800": "Person - Banner 200x800",
            "banner350x1400": "Person - Banner 350x1400",
            "publicIdentifier": "Universal name",
            "picture100x100": "Person - Picture 100x100",
            "picture200x200": "Person - Picture 200x200",
            "picture400x400": "Person - Picture 400x400",
            "picture800x800": "Person - Picture 800x800",
            "premiumSubscriber": "Premium subscriber?"
        }
        df_profile_data.rename(columns = profile_data_rename_dict, inplace = True)
        return df_profile_data
    def send_message_using_vmid(dataframe, waiting_time_min, waiting_time_max, message_column_name, vmid_column_name, result_column_name):
        profile_data = get_user_profile()
        premiumSubscriber = safe_extract(profile_data, "premiumSubscriber")
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_send_message_using_vmid = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def send_message_and_store_result_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            send_message(row[message_column_name], get_conversation_id_using_vmid(row[vmid_column_name]), premiumSubscriber)
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_send_message_using_vmid.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Sending messages")
        dataframe[result_column_name] = dataframe.progress_apply(send_message_and_store_result_with_waiting, axis=1)
        return dataframe
    def mark_conversation_as_seen_using_conversation_id(dataframe, waiting_time_min, waiting_time_max, conversation_id_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_mark_conversation_as_seen_using_conversation_id = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def mark_conversation_as_seen_and_store_result_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            mark_conversation_as_seen(row[conversation_id_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_mark_conversation_as_seen_using_conversation_id.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Marking as seen conversations")
        dataframe[result_column_name] = dataframe.progress_apply(mark_conversation_as_seen_and_store_result_with_waiting, axis=1)
        return(dataframe)
    def get_all_connection_requests():
        #get_all_invitations
        all_invitations = get_all_invitations()
        df_all_invitations_final = pd.DataFrame()
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_all_connection_requests = st.progress(0)
            number_iterations = len(all_invitations)
            index = 0
        #--STREAMLIT--#
        for invitation in all_invitations:
            df_all_invitations_loop = pd.DataFrame()
            #Invitation
            entityUrn = safe_extract(invitation, "entityUrn")
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            totalCount = safe_extract(invitation, "insights", 0, "sharedInsight", "com.linkedin.voyager.relationships.shared.SharedConnectionsInsight", "totalCount")
            invitationType = safe_extract(invitation, "invitation", "invitationType")
            try:
                sentTime = convertToTimestamp(safe_extract(invitation, "invitation", "sentTime"))
            except:
                sentTime = safe_extract(invitation, "invitation", "sentTime")
            subtitle = None
            typeLabel = None
            title = None
            #Person
            firstName = safe_extract(invitation, "invitation", "fromMember", "firstName")
            lastName = safe_extract(invitation, "invitation", "fromMember", "lastName")
            fullName = None
            if firstName and lastName:
                fullName = firstName + " " + lastName
                fullName = fullName.strip() if fullName is not None else None
            elif firstName:
                fullName = firstName
                fullName = fullName.strip() if fullName is not None else None
            else:
                fullName = lastName
                fullName = fullName.strip() if fullName is not None else None
            dashEntityUrn = safe_extract(invitation, "invitation", "fromMember", "dashEntityUrn")
            try:
                dashEntityUrn = dashEntityUrn.split(':')[-1]
            except:
                pass
            occupation = safe_extract(invitation, "invitation", "fromMember", "occupation")
            objectUrn = safe_extract(invitation, "invitation", "fromMember", "objectUrn")
            try:
                objectUrn = objectUrn.split(':')[-1]
            except:
                pass
            background_artifacts = safe_extract(invitation, "invitation", "fromMember", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
            background_rootUrl = safe_extract(invitation, "invitation", "fromMember", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
            banner200x800 = banner350x1400 = None
            if background_artifacts and background_rootUrl:
                for artifact in background_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in file_segment:
                        banner200x800 = f"{background_rootUrl}{file_segment}"
                    elif '350_1400' in file_segment:
                        banner350x1400 = f"{background_rootUrl}{file_segment}"
                    if banner200x800 and banner350x1400:
                        break
            publicIdentifier = safe_extract(invitation, "invitation", "fromMember", "publicIdentifier")
            picture_artifacts = safe_extract(invitation, "invitation", "fromMember", "picture", "com.linkedin.common.VectorImage", "artifacts")
            picture_rootUrl = safe_extract(invitation, "invitation", "fromMember", "picture", "com.linkedin.common.VectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            #Invitation
            customMessage = safe_extract(invitation, "invitation", "customMessage")
            sharedSecret = safe_extract(invitation, "invitation", "sharedSecret")
            unseen = safe_extract(invitation, "invitation", "unseen")
            #CONTENT_SERIES
            check_invitationType = safe_extract(invitation, "genericInvitationView", "invitationType")
            if check_invitationType:
                #Invitation
                entityUrn = safe_extract(invitation, "invitation", "entityUrn")
                try:
                    entityUrn = entityUrn.split(':')[-1]
                except:
                    pass
                invitationType = safe_extract(invitation, "genericInvitationView", "invitationType")
                try:
                    sentTime = convertToTimestamp(safe_extract(invitation, "genericInvitationView", "sentTime"))
                except:
                    sentTime = safe_extract(invitation, "genericInvitationView", "sentTime")
                #Company
                dashEntityUrn = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "objectUrn")
                try:
                    dashEntityUrn = dashEntityUrn.split(':')[-1]
                except:
                    pass
                fullName = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "name")
                picture_artifacts = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "logo", "com.linkedin.common.VectorImage", "artifacts")
                picture_rootUrl = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "logo", "com.linkedin.common.VectorImage", "rootUrl")
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
                publicIdentifier = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "universalName")
                #Invitation
                subtitle = safe_extract(invitation, "genericInvitationView", "subtitle", "text")
                typeLabel = safe_extract(invitation, "genericInvitationView", "typeLabel")
                title = safe_extract(invitation, "genericInvitationView", "title", "text")
                sharedSecret = safe_extract(invitation, "genericInvitationView", "sharedSecret")
                unseen = safe_extract(invitation, "genericInvitationView", "unseen")
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["entityUrn", "totalCount", "invitationType", "sentTime", "subtitle", "typeLabel", "title", "firstName", "lastName", "fullName", "dashEntityUrn", "occupation", "objectUrn", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "customMessage", "sharedSecret", "unseen"]}
            df_all_invitations_loop = pd.DataFrame(selected_vars)
            df_all_invitations_final = pd.concat([df_all_invitations_final, df_all_invitations_loop])
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_get_all_connection_requests.progress(index / number_iterations)
            #--STREAMLIT--#
        all_invitations_rename_dict = {
            "entityUrn": "Invitation - ID",
            "totalCount": "Invitation - Shared connections count",
            "invitationType": "Invitation - Type",
            "sentTime": "Invitation - Creation time",
            "subtitle": "Invitation - Subtitle",
            "typeLabel": "Invitation - Type label",
            "title": "Invitation - Title",
            "firstName": "First name",
            "lastName": "Last name",
            "fullName": "Full name",
            "dashEntityUrn": "vmid",
            "occupation": "Occupation",
            "objectUrn": "User ID",
            "banner200x800": "Banner 200x800",
            "banner350x1400": "Banner 350x1400",
            "publicIdentifier": "Universal name",
            "picture100x100": "Picture 100x100",
            "picture200x200": "Picture 200x200",
            "picture400x400": "Picture 400x400",
            "picture800x800": "Picture 800x800",
            "customMessage": "Invitation - Custom message?",
            "sharedSecret": "Invitation - Shared secret",
            "unseen": "Invitation - Unseen?"
        }
        df_all_invitations_final.rename(columns = all_invitations_rename_dict, inplace = True)
        return df_all_invitations_final
    def accept_or_remove_connection_requests(dataframe, waiting_time_min, waiting_time_max, action, invitation_id_column_name, invitation_shared_secret_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_accept_or_remove_connection_requests = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def accept_or_ignore_and_store_result_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            reply_invitation(row[invitation_id_column_name], row[invitation_shared_secret_column_name], action, True)
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_accept_or_remove_connection_requests.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Accepting/ignoring connection requests")
        dataframe[result_column_name] = dataframe.progress_apply(accept_or_ignore_and_store_result_with_waiting, axis=1)
        return dataframe
    def send_connection_requests(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, message_column_name, result_column_name):        
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_send_connection_requests = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def send_connection_and_store_result_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            add_connection(row[vmid_column_name], row[message_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_send_connection_requests.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Sending connection requests")
        dataframe[result_column_name] = dataframe.progress_apply(send_connection_and_store_result_with_waiting, axis=1)
        return dataframe
    def remove_connections(dataframe, waiting_time_min, waiting_time_max, unique_identifier_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_remove_connections = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def send_connection_and_store_result_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            remove_connection(row[unique_identifier_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_remove_connections.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Removing connections")
        dataframe[result_column_name] = dataframe.progress_apply(send_connection_and_store_result_with_waiting, axis=1)
        return dataframe
    def follow_or_unfollow_profiles(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, action, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_follow_or_unfollow_profiles = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def follow_or_unfollow_and_store_result_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            follow_unfollow_profile("urn:li:fsd_profile:"+row[vmid_column_name], action)
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_follow_or_unfollow_profiles.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Following/unfollowing profiles")
        dataframe[result_column_name] = dataframe.progress_apply(follow_or_unfollow_and_store_result_with_waiting, axis=1)
        return dataframe
    def get_all_connections_profiles():
        #-->get_all_connections
        all_connections = get_all_connections()
        df_all_connections_final = pd.DataFrame()
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_all_connections_profiles = st.progress(0)
            number_iterations = len(all_connections)
            index = 0
        #--STREAMLIT--#
        for connection in all_connections:
            df_all_connections_loop = pd.DataFrame()
            lastName = safe_extract(connection, "connectedMemberResolutionResult", "lastName")
            firstName = safe_extract(connection, "connectedMemberResolutionResult", "firstName")
            picture_artifacts = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "artifacts")
            picture_rootUrl = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            entityUrn = safe_extract(connection, "connectedMemberResolutionResult", "entityUrn")
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            headline = safe_extract(connection, "connectedMemberResolutionResult", "headline")
            publicIdentifier = safe_extract(connection, "connectedMemberResolutionResult", "publicIdentifier")
            createdAt = convertToTimestamp(safe_extract(connection, "createdAt"))
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["firstName", "lastName", "headline", "entityUrn", "publicIdentifier", "createdAt", "picture100x100", "picture200x200", "picture400x400", "picture800x800"]}
            df_all_connections_loop = pd.DataFrame(selected_vars)
            df_all_connections_final = pd.concat([df_all_connections_final, df_all_connections_loop])
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_get_all_connections_profiles.progress(index / number_iterations)
            #--STREAMLIT--#
        all_conversations_rename_dict = {
            "firstName": "First name",
            "lastName": "Last name",
            "headline": "Headline",
            "entityUrn": "vmid",
            "publicIdentifier": "Universal name",
            "createdAt": "Creation time",
            "picture100x100": "Picture 100x100",
            "picture200x200": "Picture 200x200",
            "picture400x400": "Picture 400x400",
            "picture800x800": "Picture 800x800"
        }
        df_all_connections_final.rename(columns = all_conversations_rename_dict, inplace = True)
        df_all_connections_final = df_all_connections_final.dropna(subset=['vmid'])
        df_all_connections_final = df_all_connections_final[df_all_connections_final['vmid'] != '']
        return df_all_connections_final
    def get_all_conversations_with_connections(waiting_time_min, waiting_time_max):
        #-->get_all_connections
        all_connections = get_all_connections()
        df_all_connections_final = pd.DataFrame()
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_all_conversations_with_connections = st.progress(0)
            number_iterations = len(all_connections)
            index = 0
        #--STREAMLIT--#
        for connection in all_connections:
            df_all_connections_loop = pd.DataFrame()
            lastName = safe_extract(connection, "connectedMemberResolutionResult", "lastName")
            firstName = safe_extract(connection, "connectedMemberResolutionResult", "firstName")
            picture_artifacts = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "artifacts")
            picture_rootUrl = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            entityUrn = safe_extract(connection, "connectedMemberResolutionResult", "entityUrn")
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            headline = safe_extract(connection, "connectedMemberResolutionResult", "headline")
            publicIdentifier = safe_extract(connection, "connectedMemberResolutionResult", "publicIdentifier")
            createdAt = convertToTimestamp(safe_extract(connection, "createdAt"))
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["firstName", "lastName", "headline", "entityUrn", "publicIdentifier", "createdAt", "picture100x100", "picture200x200", "picture400x400", "picture800x800"]}
            df_all_connections_loop = pd.DataFrame(selected_vars)
            df_all_connections_final = pd.concat([df_all_connections_final, df_all_connections_loop])
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_get_all_conversations_with_connections.progress(index / number_iterations)
            #--STREAMLIT--#
        all_conversations_rename_dict = {
            "firstName": "First name",
            "lastName": "Last name",
            "headline": "Headline",
            "entityUrn": "vmid",
            "publicIdentifier": "Universal name",
            "createdAt": "Creation time",
            "picture100x100": "Picture 100x100",
            "picture200x200": "Picture 200x200",
            "picture400x400": "Picture 400x400",
            "picture800x800": "Picture 800x800"
        }
        df_all_connections_final.rename(columns = all_conversations_rename_dict, inplace = True)
        df_all_connections_final.replace('', np.nan, inplace = True)
        columns_to_ignore = {'Creation time'}
        df_all_connections_final.dropna(how = 'all', subset = [col for col in df_all_connections_final.columns if col not in columns_to_ignore], inplace = True)
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #get_all_conversations_with_connections
        def get_all_conversations_with_connections(vmid):
            conversation_id = get_conversation_id_using_vmid(vmid)
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return conversation_id
        tqdm.pandas(desc="Searching for conversation IDs")
        df_all_connections_final['Conversation ID'] = df_all_connections_final['vmid'].progress_apply(get_all_conversations_with_connections)
        return df_all_connections_final
    def get_all_sent_connection_requests():
        #get_all_sent_invitations
        all_sent_invitations = get_all_sent_invitations()
        df_all_sent_invitations_final = pd.DataFrame()
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_all_connection_requests = st.progress(0)
            number_iterations = len(all_sent_invitations)
            index = 0
        #--STREAMLIT--#
        for invitations in all_sent_invitations:
            df_all_sent_invitations_loop = pd.DataFrame()
            #Invitee
            invitee_cardActionTarget = safe_extract(invitations, "cardActionTarget")
            #Invitation
            sentTimeLabel = safe_extract(invitations, "sentTimeLabel")
            try:
                sentTimeLabel = convertToTimestamp(sentTimeLabel)
            except:
                sentTimeLabel = safe_extract(invitations, "sentTimeLabel")
            #Invitee
            invitee_firstName = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "firstName")
            invitee_lastName = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "lastName")
            picture_artifacts = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "profilePicture", "displayImageReferenceResolutionResult", "vectorImage", "artifacts")
            picture_rootUrl = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "profilePicture", "displayImageReferenceResolutionResult", "vectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            invitee_entityUrn = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "entityUrn")
            try:
                invitee_entityUrn = invitee_entityUrn.split(':')[-1]
            except:
                pass
            #Invitation
            inviterFollowingInvitee = safe_extract(invitations, "invitation", "inviterFollowingInvitee")
            genericInvitationType = safe_extract(invitations, "invitation", "genericInvitationType")
            invitationState = safe_extract(invitations, "invitation", "invitationState")
            invitationId = safe_extract(invitations, "invitation", "invitationId")
            if invitationId:
                invitationId = str(invitationId)
            message = safe_extract(invitations, "invitation", "message")
            #Inviter
            inviter_firstName = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "firstName")
            inviter_lastName = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "lastName")
            inviter_objectUrn = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "objectUrn")
            try:
                inviter_objectUrn = inviter_objectUrn.split(':')[-1]
            except:
                pass
            inviter_entityUrn = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "entityUrn")
            try:
                inviter_entityUrn = inviter_entityUrn.split(':')[-1]
            except:
                pass
            try:
                inviter_entityUrn = inviter_entityUrn.split(':')[-1]
            except:
                pass
            inviter_publicIdentifier = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "publicIdentifier")
            #Invitation
            invitationType = safe_extract(invitations, "invitation", "invitationType")
            #Invitee
            subtitle = safe_extract(invitations, "subtitle", "text")
            title = safe_extract(invitations, "title", "text")
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["invitationId", "sentTimeLabel", "message", "inviterFollowingInvitee", "genericInvitationType", "invitationType", "invitationState", "invitee_firstName", "invitee_lastName", "title", "subtitle", "invitee_cardActionTarget", "invitee_entityUrn", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "inviter_firstName", "inviter_lastName", "inviter_objectUrn", "inviter_entityUrn", "inviter_publicIdentifier"]}
            df_all_sent_invitations_loop = pd.DataFrame(selected_vars)
            df_all_sent_invitations_final = pd.concat([df_all_sent_invitations_final, df_all_sent_invitations_loop])
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_get_all_connection_requests.progress(index / number_iterations)
            #--STREAMLIT--#
        all_sent_invitations_rename_dict = {
            "invitationId": "Invitation - ID",
            "sentTimeLabel": "Invitation - Sent time",
            "message": "Invitation - Message",
            "inviterFollowingInvitee": "Invitation - Inviter following invitee?",
            "genericInvitationType": "Invitation - Generic invitation type",
            "invitationType": "Invitation - Type",
            "invitationState": "Invitation - State",
            "invitee_firstName": "Invitee - First name",
            "invitee_lastName": "Invitee - Last name",
            "title": "Invitee - Full name",
            "subtitle": "Invitee - Occupation",
            "invitee_cardActionTarget": "Invitee - Public LinkedIn URL",
            "invitee_entityUrn": "Invitee - vmid",
            "picture100x100": "Invitee - Picture 100x100",
            "picture200x200": "Invitee - Picture 200x200",
            "picture400x400": "Invitee - Picture 400x400",
            "picture800x800": "Invitee - Picture 800x800",
            "inviter_firstName": "Inviter - First name",
            "inviter_lastName": "Inviter - Last name",
            "inviter_objectUrn": "Inviter - User ID",
            "inviter_entityUrn": "Invitee - vmid",
            "inviter_publicIdentifier": "Invitee - Universal name"
        }
        df_all_sent_invitations_final.rename(columns = all_sent_invitations_rename_dict, inplace = True)
        return df_all_sent_invitations_final
    def withdraw_connection_requests(dataframe, waiting_time_min, waiting_time_max, invitation_id_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #--STREAMLIT--#
        if streamlit_execution:
            progress_bar_get_all_connection_requests = st.progress(0)
            number_iterations = len(dataframe)
            index = 0
        #--STREAMLIT--#
        def withdraw_invitation_with_waiting(row):
            #--STREAMLIT--#
            nonlocal index
            #--STREAMLIT--#
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            withdraw_invitation(row[invitation_id_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            #--STREAMLIT--#
            if streamlit_execution:
                index += 1
                progress_bar_get_all_connection_requests.progress(index / number_iterations)
            #--STREAMLIT--#
            return result.strip()
        tqdm.pandas(desc="Withdrawing connection requests")
        dataframe[result_column_name] = dataframe.progress_apply(withdraw_invitation_with_waiting, axis=1)
        return dataframe
    #Check script_type
    if script_type == "get_last_20_conversations":
        dataframe_result = get_last_20_conversations()
        return dataframe_result
    if script_type == "get_all_messages_from_conversation":
        dataframe_result = get_all_messages_from_conversation(dataframe, conversation_id_column_name)
        return dataframe_result
    if script_type == "obtain_current_user_profile":
        dataframe_result = obtain_current_user_profile()
        return dataframe_result
    if script_type == "send_message_using_vmid":
        dataframe_result = send_message_using_vmid(dataframe, waiting_time_min, waiting_time_max, message_column_name, vmid_column_name, result_column_name)
        return dataframe_result
    if script_type == "mark_conversation_as_seen_using_conversation_id":
        dataframe_result = mark_conversation_as_seen_using_conversation_id(dataframe, waiting_time_min, waiting_time_max, conversation_id_column_name, result_column_name)
        return dataframe_result
    if script_type == "get_all_connection_requests":
        dataframe_result = get_all_connection_requests()
        return dataframe_result
    if script_type == "accept_or_remove_connection_requests":
        dataframe_result = accept_or_remove_connection_requests(dataframe, waiting_time_min, waiting_time_max, action, invitation_id_column_name, invitation_shared_secret_column_name, result_column_name)
        return dataframe_result
    if script_type == "send_connection_requests":
        dataframe_result = send_connection_requests(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, message_column_name, result_column_name)
        return dataframe_result
    if script_type == "remove_connections":
        dataframe_result = remove_connections(dataframe, waiting_time_min, waiting_time_max, unique_identifier_column_name, result_column_name)
        return dataframe_result
    if script_type == "follow_or_unfollow_profiles":
        dataframe_result = follow_or_unfollow_profiles(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, action, result_column_name)
        return dataframe_result
    if script_type == "get_all_connections_profiles":
        dataframe_result = get_all_connections_profiles()
        return dataframe_result
    if script_type == "get_all_conversations_with_connections":
        dataframe_result = get_all_conversations_with_connections(waiting_time_min, waiting_time_max)
        return dataframe_result
    if script_type == "get_all_sent_connection_requests":
        dataframe_result = get_all_sent_connection_requests()
        return dataframe_result
    if script_type == "withdraw_connection_requests":
        dataframe_result = withdraw_connection_requests(dataframe, waiting_time_min, waiting_time_max, invitation_id_column_name, result_column_name)
        return dataframe_result