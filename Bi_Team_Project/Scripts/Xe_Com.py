import re
import XE_User_Config
from robobrowser import RoboBrowser
import requests
url_site = "https://id.corp.aol.com/identity/XUI/#login/&realm=/aolcorporate/aolexternals&goto=https%3A%2F%2Fid.corp.aol.com%2Fidentity%2Foauth2%2Fauthorize%3Fscope%3Dopenid%26response_type%3Dcode%26realm%3Daolcorporate%252Faolexternals%26redirect_uri%3Dhttps%253A%252F%252Fconsole.onedisplaymp.aol.com%252Fh2%252Fdo%252Fauth%26client_id%3D70cf1570-2be3-4fa7-bc00-681eb7a80f5b%26state%3D238t74z73zt"
br = RoboBrowser()
br.open(url_site)
form = br.get_form()
form['Username'] = XE_User_Config.AOL_Username
form['Password'] = XE_User_Config.AOL_Password
br.submit_form(form)
src = str(br.parsed())

