import pandas as pd
import random
import requests
import time
import urllib.parse
from tqdm import tqdm
from urllib.parse import quote, urlencode

def linkedin_search_scripts(
    csrf_token: str = None, 
    dataframe: pd.DataFrame = None, 
    script_type: str = None, 
    first_name_column_name: str = None, 
    last_name_column_name: int = None, 
    company_name_column_name: int = None, 
    query_column_name: str = None, 
    company_column_name: str = None, 
    cookies_dict: dict = None, 
) -> None:
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    MAX_SEARCH_COUNT = 49
    MAX_REPEATED_REQUESTS = 3
    def get_id_from_urn(urn):
        return urn.split(":")[3]
    def extract_vmid(urn_id):
        if urn_id is not None:
            return urn_id[:39]
        return None
    def extract_profile_url(url):
        if url is not None:
            return url.split('?')[0] + '/'
        return url
    def create_vmid_url(vmid):
        return f"https://www.linkedin.com/in/{vmid}/" if vmid else None
    def create_maincompanyid_url(maincompanyid):
        return f"https://www.linkedin.com/company/{maincompanyid}/" if maincompanyid else None
    def fetch(url):
        response = requests.get(
            "https://www.linkedin.com/voyager/api"+url, headers=headers, cookies=cookies_dict)
        response.raise_for_status()
        return response
    def search(params, limit=-1, offset=0):
        """
        Perform a LinkedIn search.

        :param params: Search parameters (see code)
        :type params: dict
        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional
        :param offset: Index to start searching from
        :type offset: int, optional

        :return: List of search results
        :rtype: list
        """
        count = MAX_SEARCH_COUNT
        if limit is None:
            limit = -1
        results = []
        while True:
            if limit > -1 and limit - len(results) < count:
                count = limit - len(results)
            default_params = {
                "count": str(count),
                "filters": "List()",
                "origin": "GLOBAL_SEARCH_HEADER",
                "q": "all",
                "start": len(results) + offset,
                "queryContext": "List(spellCorrectionEnabled->true,relatedSearchesEnabled->true,kcardTypes->PROFILE|COMPANY)",
                "includeWebMetadata": "true",
            }
            default_params.update(params)
            keywords = f"keywords:{urllib.parse.quote(default_params['keywords'])}," if "keywords" in default_params else ""
            res = fetch(
                f"/graphql?variables=(start:{default_params['start']},origin:{default_params['origin']},"
                f"query:("
                f"{keywords}"
                f"flagshipSearchIntent:SEARCH_SRP,"
                f"queryParameters:{default_params['filters']},"
                f"includeFiltersInResponse:false))&queryId=voyagerSearchDashClusters"
                f".b0928897b71bd00a5a7291755dcd64f0"
            )
            data = res.json()
            data_clusters = data.get("data", {}).get("searchDashClustersByAll", {})
            if not data_clusters:
                return []
            if data_clusters.get("_type") != "com.linkedin.restli.common.CollectionResponse":
                return []
            new_elements = []
            for it in data_clusters.get("elements", []):
                if it.get("_type") != "com.linkedin.voyager.dash.search.SearchClusterViewModel":
                    continue
                for el in it.get("items", []):
                    if el.get("_type") != "com.linkedin.voyager.dash.search.SearchItem":
                        continue
                    e = el.get("item", {}).get("entityResult", {})
                    if not e:
                        continue
                    if e.get("_type") != "com.linkedin.voyager.dash.search.EntityResultViewModel":
                        continue
                    new_elements.append(e)
            results.extend(new_elements)
            if (-1 < limit <= len(results)) or len(results) / count >= MAX_REPEATED_REQUESTS or not new_elements:
                break
        return results
    def search_people(keywords=None, connection_of=None, network_depths=None, current_company=None,
                    past_companies=None, nonprofit_interests=None, profile_languages=None, regions=None,
                    industries=None, schools=None, contact_interests=None, service_categories=None,
                    include_private_profiles=False, keyword_first_name=None, keyword_last_name=None,
                    keyword_title=None, keyword_company=None, keyword_school=None, network_depth=None, title=None,
                    **kwargs):
        """
        Perform a LinkedIn search for people.

        :param keywords: Keywords to search on
        :type keywords: str, optional
        :param current_company: A list of company URN IDs (str)
        :type current_company: list, optional
        :param past_companies: A list of company URN IDs (str)
        :type past_companies: list, optional
        :param regions: A list of geo URN IDs (str)
        :type regions: list, optional
        :param industries: A list of industry URN IDs (str)
        :type industries: list, optional
        :param schools: A list of school URN IDs (str)
        :type schools: list, optional
        :param profile_languages: A list of 2-letter language codes (str)
        :type profile_languages: list, optional
        :param contact_interests: A list containing one or both of "proBono" and "boardMember"
        :type contact_interests: list, optional
        :param service_categories: A list of service category URN IDs (str)
        :type service_categories: list, optional
        :param network_depth: Deprecated, use `network_depths`. One of "F", "S" and "O" (first, second and third+ respectively)
        :type network_depth: str, optional
        :param network_depths: A list containing one or many of "F", "S" and "O" (first, second and third+ respectively)
        :type network_depths: list, optional
        :param include_private_profiles: Include private profiles in search results. If False, only public profiles are included. Defaults to False
        :type include_private_profiles: boolean, optional
        :param keyword_first_name: First name
        :type keyword_first_name: str, optional
        :param keyword_last_name: Last name
        :type keyword_last_name: str, optional
        :param keyword_title: Job title
        :type keyword_title: str, optional
        :param keyword_company: Company name
        :type keyword_company: str, optional
        :param keyword_school: School name
        :type keyword_school: str, optional
        :param connection_of: Connection of LinkedIn user, given by profile URN ID
        :type connection_of: str, optional
        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional

        :return: List of profiles (minimal data only)
        :rtype: list
        """
        filters = ["(key:resultType,value:List(PEOPLE))"]
        if connection_of:
            filters.append(f"(key:connectionOf,value:List({quote(connection_of)})")
        if network_depths:
            stringify = " | ".join(map(quote, network_depths))
            filters.append(f"(key:network,value:List({stringify}))")
        elif network_depth:
            filters.append(f"(key:network,value:List({quote(network_depth)}))")
        if regions:
            stringify = " | ".join(map(quote, regions))
            filters.append(f"(key:geoUrn,value:List({stringify}))")
        if industries:
            stringify = " | ".join(map(quote, industries))
            filters.append(f"(key:industry,value:List({stringify}))")
        if current_company:
            stringify = " | ".join(map(quote, current_company))
            filters.append(f"(key:currentCompany,value:List({stringify}))")
        if past_companies:
            stringify = " | ".join(map(quote, past_companies))
            filters.append(f"(key:pastCompany,value:List({stringify}))")
        if profile_languages:
            stringify = " | ".join(map(quote, profile_languages))
            filters.append(f"(key:profileLanguage,value:List({stringify}))")
        if nonprofit_interests:
            stringify = " | ".join(map(quote, nonprofit_interests))
            filters.append(f"(key:nonprofitInterest,value:List({stringify}))")
        if schools:
            stringify = " | ".join(map(quote, schools))
            filters.append(f"(key:schools,value:List({stringify}))")
        if service_categories:
            stringify = " | ".join(map(quote, service_categories))
            filters.append(f"(key:serviceCategory,value:List({stringify}))")
        keyword_title = keyword_title if keyword_title else title
        if keyword_first_name:
            filters.append(
                f"(key:firstName,value:List({quote(keyword_first_name)}))")
        if keyword_last_name:
            filters.append(
                f"(key:lastName,value:List({quote(keyword_last_name)}))")
        if keyword_title:
            filters.append(f"(key:title,value:List({quote(keyword_title)}))")
        if keyword_company:
            filters.append(f"(key:company,value:List({quote(keyword_company)}))")
        if keyword_school:
            filters.append(f"(key:school,value:List({quote(keyword_school)}))")
        params = {"filters": "List({})".format(",".join(filters))}
        if keywords:
            params["keywords"] = keywords
        data = search(params, **kwargs)
        results = []
        for item in data:
            if not include_private_profiles and (item.get("entityCustomTrackingInfo") or {}).get("memberDistance") == "OUT_OF_NETWORK":
                continue
            results.append(
                {
                    "urn_id": item.get("entityUrn").split(':')[-1] if item.get("entityUrn") else None,
                    "distance": (item.get("entityCustomTrackingInfo") or {}).get("memberDistance"),
                    "jobtitle": (item.get("primarySubtitle") or {}).get("text"),
                    "location": (item.get("secondarySubtitle") or {}).get("text"),
                    "name": (item.get("title") or {}).get("text"),
                    "profile_url": item.get("navigationUrl"),
                }
            )
        return results
    def search_companies(keywords=None, **kwargs):
        """
        Perform a LinkedIn search for companies.

        :param keywords: A list of search keywords (str)
        :type keywords: list, optional

        :return: List of companies
        :rtype: list
        """
        filters = ["(key:resultType,value:List(COMPANIES))"]
        params = {
            "filters": "List({})".format(",".join(filters)),
            "queryContext": "List(spellCorrectionEnabled->true)",
        }
        if keywords:
            params["keywords"] = keywords
        data = search(params, **kwargs)
        results = []
        for item in data:
            if "company" not in item.get("trackingUrn"):
                continue
            results.append(
                {
                    "urn_id": get_id_from_urn(item.get("trackingUrn", None)),
                    "name": (item.get("title") or {}).get("text", None),
                    "headline": (item.get("primarySubtitle") or {}).get("text", None),
                    "subline": (item.get("secondarySubtitle") or {}).get("text", None),
                }
            )
        return results
    def search_jobs(keywords=None, companies=None, experience=None, job_type=None, job_title=None,
                    industries=None, location_name=None, remote=None, listed_at=24 * 60 * 60, distance=None, limit=-1,
                    offset=0, **kwargs,):
        """
        Perform a LinkedIn search for jobs.

        :param keywords: Search keywords (str)
        :type keywords: str, optional
        :param companies: A list of company URN IDs (str)
        :type companies: list, optional
        :param experience: A list of experience levels, one or many of "1", "2", "3", "4", "5" and "6" (internship, entry level, associate, mid-senior level, director and executive, respectively)
        :type experience: list, optional
        :param job_type:  A list of job types , one or many of "F", "C", "P", "T", "I", "V", "O" (full-time, contract, part-time, temporary, internship, volunteer and "other", respectively)
        :type job_type: list, optional
        :param job_title: A list of title URN IDs (str)
        :type job_title: list, optional
        :param industries: A list of industry URN IDs (str)
        :type industries: list, optional
        :param location_name: Name of the location to search within. Example: "Kyiv City, Ukraine"
        :type location_name: str, optional
        :param remote: Filter for remote jobs, onsite or hybrid. onsite:"1", remote:"2", hybrid:"3"
        :type remote: list, optional
        :param listed_at: maximum number of seconds passed since job posting. 86400 will filter job postings posted in last 24 hours.
        :type listed_at: int/str, optional. Default value is equal to 24 hours.
        :param distance: maximum distance from location in miles
        :type distance: int/str, optional. If not specified, None or 0, the default value of 25 miles applied.
        :param limit: maximum number of results obtained from API queries. -1 means maximum which is defined by constants and is equal to 1000 now.
        :type limit: int, optional, default -1
        :param offset: indicates how many search results shall be skipped
        :type offset: int, optional
        :return: List of jobs
        :rtype: list
        """
        count = MAX_SEARCH_COUNT
        if limit is None:
            limit = -1
        query = {"origin": "JOB_SEARCH_PAGE_QUERY_EXPANSION"}
        if keywords:
            query["keywords"] = quote(keywords)
        if location_name:
            query["locationFallback"] = quote(location_name)
        query["selectedFilters"] = {}
        if companies:
            query["selectedFilters"]["company"] = f"List({','.join(map(quote, companies))})"
        if experience:
            query["selectedFilters"]["experience"] = f"List({','.join(map(quote, experience))})"
        if job_type:
            query["selectedFilters"]["jobType"] = f"List({','.join(map(quote, job_type))})"
        if job_title:
            query["selectedFilters"]["title"] = f"List({','.join(map(quote, job_title))})"
        if industries:
            query["selectedFilters"]["industry"] = f"List({','.join(map(quote, industries))})"
        if distance:
            query["selectedFilters"]["distance"] = f"List({quote(distance)})"
        if remote:
            query["selectedFilters"]["workplaceType"] = f"List({','.join(map(quote, remote))})"
        query["selectedFilters"]["timePostedRange"] = f"List(r{listed_at})"
        query["spellCorrectionEnabled"] = "true"
        query = (
            str(query)
            .replace(" ", "")
            .replace("'", "")
            .replace("KEYWORD_PLACEHOLDER", keywords or "")
            .replace("LOCATION_PLACEHOLDER", location_name or "")
            .replace("{", "(")
            .replace("}", ")")
        )
        results = []
        while True:
            if limit > -1 and limit - len(results) < count:
                count = limit - len(results)
            default_params = {
                "decorationId": "com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-174",
                "count": count,
                "q": "jobSearch",
                "query": query,
                "start": len(results) + offset,
            }
            res = fetch(
                f"/voyagerJobsDashJobCards?{urlencode(default_params, safe='(),:')}",
            )
            data = res.json()
            elements = data.get("included", [])
            new_data = [
                i
                for i in elements
                if i["$type"] == "com.linkedin.voyager.dash.jobs.JobPosting"
            ]
            if not new_data:
                break
            results.extend(new_data)
            if (
                (-1 < limit <= len(results))
                or len(results) / count >= MAX_REPEATED_REQUESTS
            ) or len(elements) == 0:
                break
        return results
    def people_search_first_name_last_name_company_name(dataframe, first_name_column_name, last_name_column_name, company_name_column_name):
        dataframe_final = pd.DataFrame()
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0], desc='Processing rows'):
            dataframe_loop = pd.DataFrame()
            keyword_first_name = row[first_name_column_name]
            keyword_last_name = row[last_name_column_name]
            keyword_company = row[company_name_column_name]
            error = None
            person_result = search_people(keyword_first_name=keyword_first_name,
                                        keyword_last_name=keyword_last_name, keyword_company=keyword_company, limit=5)
            first_person_result = safe_extract(person_result, 0)
            if first_person_result:
                urn_id = safe_extract(first_person_result, 'urn_id')
                vmid = extract_vmid(urn_id)
                linkedin_url = create_vmid_url(vmid)
                distance = safe_extract(first_person_result, 'distance')
                jobtitle = safe_extract(first_person_result, 'jobtitle')
                location = safe_extract(first_person_result, 'location')
                name = safe_extract(first_person_result, 'name')
                profile_url = safe_extract(first_person_result, 'profile_url')
                public_linkedin_url = extract_profile_url(profile_url)
            if not first_person_result:
                error = "No results found"
                vmid = linkedin_url = distance = jobtitle = location = name = public_linkedin_url = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["keyword_first_name", "keyword_last_name", "keyword_company",
                                                                "error", "vmid", "linkedin_url", "distance", "jobtitle", "location", "name", "public_linkedin_url"]}
            dataframe_loop = pd.DataFrame(selected_vars)
            dataframe_final = pd.concat([dataframe_final, dataframe_loop])
            time.sleep(random.uniform(10, 17))
        all_conversations_rename_dict = {
            "keyword_first_name": first_name_column_name,
            "keyword_last_name": last_name_column_name,
            "keyword_company": company_name_column_name,
            "error": "Error",
            "vmid": "vmid",
            "linkedin_url": "LinkedIn URL",
            "distance": "Distance",
            "jobtitle": "Job title",
            "location": "Location",
            "name": "Full name",
            "public_linkedin_url": "Public LinkedIn URL",
        }
        dataframe_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return dataframe_final
    def people_search_any_query(dataframe, query_column_name):
        dataframe_final = pd.DataFrame()
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0], desc='Processing rows'):
            dataframe_loop = pd.DataFrame()
            keywords = row[query_column_name]
            error = None
            person_result = search_people(keywords=keywords, limit=5)
            first_person_result = safe_extract(person_result, 0)
            if first_person_result:
                urn_id = safe_extract(first_person_result, 'urn_id')
                vmid = extract_vmid(urn_id)
                linkedin_url = create_vmid_url(vmid)
                distance = safe_extract(first_person_result, 'distance')
                jobtitle = safe_extract(first_person_result, 'jobtitle')
                location = safe_extract(first_person_result, 'location')
                name = safe_extract(first_person_result, 'name')
                profile_url = safe_extract(first_person_result, 'profile_url')
                public_linkedin_url = extract_profile_url(profile_url)
            if not first_person_result:
                error = "No results found"
                vmid = linkedin_url = distance = jobtitle = location = name = public_linkedin_url = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["keywords", "error", "vmid",
                                                                "linkedin_url", "distance", "jobtitle", "location", "name", "public_linkedin_url"]}
            dataframe_loop = pd.DataFrame(selected_vars)
            dataframe_final = pd.concat([dataframe_final, dataframe_loop])
            time.sleep(random.uniform(10, 17))
        all_conversations_rename_dict = {
            "keywords": query_column_name,
            "error": "Error",
            "vmid": "vmid",
            "linkedin_url": "LinkedIn URL",
            "distance": "Distance",
            "jobtitle": "Job title",
            "location": "Location",
            "name": "Full name",
            "public_linkedin_url": "Public LinkedIn URL",
        }
        dataframe_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return dataframe_final
    def company_search_company_name(dataframe, company_column_name):
        dataframe_final = pd.DataFrame()
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0], desc='Processing rows'):
            dataframe_loop = pd.DataFrame()
            keywords = row[company_column_name]
            error = None
            company_result = search_companies(keywords=keywords)
            first_company_result = safe_extract(company_result, 0)
            if first_company_result:
                urn_id = safe_extract(first_company_result, 'urn_id')
                linkedin_url = create_maincompanyid_url(urn_id)
                name = safe_extract(first_company_result, 'name')
                headline = safe_extract(first_company_result, 'headline')
                subline = safe_extract(first_company_result, 'subline')
            if not first_company_result:
                error = "No results found"
                urn_id = linkedin_url = name = headline = subline = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in [
                "keywords", "error", "urn_id", "linkedin_url", "name", "headline", "subline"]}
            dataframe_loop = pd.DataFrame(selected_vars)
            dataframe_final = pd.concat([dataframe_final, dataframe_loop])
            time.sleep(random.uniform(10, 17))
        all_conversations_rename_dict = {
            "keywords": company_column_name,
            "error": "Error",
            "urn_id": "Company ID",
            "linkedin_url": "LinkedIn URL",
            "name": "Company name",
            "headline": "Headline",
            "subline": "Subline",
        }
        dataframe_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return dataframe_final
    if script_type == "people_search_first_name_last_name_company_name":
        dataframe_final = people_search_first_name_last_name_company_name(dataframe, first_name_column_name, last_name_column_name, company_name_column_name)
        return dataframe_final
    if script_type == "people_search_any_query":
        dataframe_final = people_search_any_query(dataframe, query_column_name)
        return dataframe_final
    if script_type == "company_search_company_name":
        dataframe_final = company_search_company_name(dataframe, company_column_name)
        return dataframe_final