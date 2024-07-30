from base import *
from utils.logger import *
from utils.processes import ProcessHandler
from utils.auto_update import Update
from plex_debrid_.download import get_latest_release, parse_repo_info


class PlexDebridUpdate(Update, ProcessHandler):
    def __init__(self):
        Update.__init__(self)
        ProcessHandler.__init__(self, self.logger)
            
    def start_process(self, process_name, config_dir="/"):        
        super().start_process(process_name, config_dir, ['python', './plex_debrid/main.py', '--config-dir', '/config'])
          
    def extract_version_from_ui_settings(self):
        file_path='./plex_debrid/ui/ui_settings.py'
        with open(file_path, 'r') as file:
            file_content = file.read()
        
        tree = ast.parse(file_content)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'version':
                        return node.value.elts[0].s

        raise ValueError("Version not found in the specified file")

    def update_check(self, process_name):
        self.logger.info(f"Checking for available {process_name} updates")
    
        if not os.getenv('PD_REPO'):
            self.logger.error("PD_REPO environment variable is not set.")
            return False
        else: 
            try:
                username, repository, branch = parse_repo_info('PD_REPO')
            except ValueError as e:
                self.logger.error(str(e))
                return False

        repo_url = f"https://github.com/{username}/{repository}/archive/refs/heads/{branch}.zip"
        settings_url = f"https://raw.githubusercontent.com/{username}/{repository}/{branch}/ui/ui_settings.py"
        self.logger.debug(f"Repository URL for {process_name} update: {repo_url}")  
        self.logger.debug(f"Settings URL for {process_name} update: {settings_url}") 
    
        try:
            current_ui_version = self.extract_version_from_ui_settings()
            self.logger.info(f"Currently installed version of {process_name}: [v{current_ui_version}]")
        except ValueError as e:
            self.logger.error(f"Error reading ui_settings.py: {e}")
            return False
    
        try:
            response = requests.get(settings_url, timeout=5)
            response = response.content.decode('utf8')
            if regex.search(r"(?<=')([0-9]+\.[0-9]+)(?=')", response):
                latest_version = regex.search(r"(?<=')([0-9]+\.[0-9]+)(?=')", response).group()
                self.logger.info(f"Latest version of {process_name}: [v{latest_version}]")
                if float(current_ui_version) < float(latest_version):
                    success = get_latest_release()
                    if not success:
                        raise Exception(f"Failed to download and extract the release for {process_name}.")                    
                    else:    
                        self.logger.info(f"Automatic update installed for {process_name} [v{latest_version}]")                        
                        self.stop_process(process_name)
                        self.start_process(process_name)
                        return True
                else:
                    self.logger.info(f"Automatic update not required for {process_name}")
                    return False
        except Exception as e:
            self.logger.error(f"Automatic update failed for {process_name}: {e}")
            return False