import boto3
import json
import socket
import ssl
from urllib.error import URLError, HTTPError
import subprocess

#################
### Functions ###
#################

def get_certificate_info(context, url, port):
    try:
        # Create socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as sock:
            with context.wrap_socket(sock, server_hostname=url) as ssock:
                ssock.connect((url, int(port)))
                full_cert = ssock.getpeercert()
                print("Version: " + ssock.version())
                print("Server hostname: " + ssock.server_hostname)
                if 'serialNumber' in full_cert.keys():
                  return {
                    'domainName': url,
                    'serialNumber': full_cert['serialNumber'],
                    'notBefore': full_cert['notBefore'],
                    'notAfter': full_cert['notAfter'],
                  }
        os_call = subprocess.run(["openssl","s_client","-connect","<HOSTNAME>:443","-servername","<HOSTNAME>","-showcerts"],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,stdin=subprocess.PIPE)
        os_call.input="Q\n"
        print(os_call)
        bin_cert = ""
        cert_start = False
        cert_end = False
        for line in os_call.stdout:
          print(line)
          if line == '-----BEGIN CERTIFICATE-----':
            cert_start = True
          if line == '-----END CERTIFICATE-----':
            cert_end = True
          if cert_start and not cert_end:
            bin_cert += line
          if cert_end:
            bin_cert += '-----END CERTIFICATE-----'
            break
        print(bin_cert)
    except (URLError, HTTPError, ssl.SSLError, socket.error) as err:
        return {
          'domainName': url,
          'serialNumber': 'N/A',
          'notBefore': 'N/A',
          'notAfter': 'N/A',
          'Error': "Catched exception {0}".format(err)
        }

######################
### Main Algorythm ###
######################

# Read Input JSON file
inputs = json.loads(open('endpoints.json').read())

# Create context
context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
for endpoint in inputs['endpoints']:
    url = endpoint['url']
    port = int(endpoint['port'])
    if 'cafile' in endpoint.keys():
        # Load Cert file
        print('Loading cert chains on ' + endpoint['cafile'])
        context.load_verify_locations(endpoint['cafile'])
    else:
        print('Loading default cert chains')
        context.load_default_certs()
    print('Trying ' + url + ' on port ' + str(port))
    # Get certificate info
    print(json.dumps(get_certificate_info(context, url, port),indent=2))
