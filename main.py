import os
import watchdog.events
import watchdog.observers
import shutil
import time
import pandas as pd


class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=None, ignore_patterns=None,
                                                             ignore_directories=True, case_sensitive=True)

    def on_created(self, event):
        file_path = str(event.src_path)
        get_file = file_path.split('.')
        file_extent = get_file[1]
        dirs_split = file_path.split("\\")
        folder = dirs_split[:-1]
        folder_path = "\\".join(folder)
        x = file_path.split("\\")
        y = x[-1:]
        filename = ''.join(y)

        if file_extent == "xlsx":

            print(f"We found an excel file in a monitored folder {folder_path}")
            print(f"excel file created at {file_path}")
            new_xl = file_path
            master_xl_file = os.path.join(master_xl, 'master.xlsx')
            df1 = pd.read_excel(master_xl_file)
            df2 = pd.read_excel(new_xl)
            print(df1)
            print(df2)
            values1 = df1[['CEDULA', 'NOMBRE COMPLETO DEL ESTUDIANTE', 'EDAD', ' VACUNA VIRUS DEL PAPILOMA HUMANO(VPH)',
                           ' VACUNA ANTITETANICA(DT) de los 10 años']]
            values2 = df2[['CEDULA', 'NOMBRE COMPLETO DEL ESTUDIANTE', 'EDAD', ' VACUNA VIRUS DEL PAPILOMA HUMANO(VPH)',
                           ' VACUNA ANTITETANICA(DT) de los 10 años']]
            data_frame = [values1, values2]
            join = pd.concat(data_frame)
            join.to_excel(master_xl_file)

            if not os.path.exists(os.path.join(processed_file_dest, filename)):
                print(f"{processed_file_dest}no existe")
                shutil.move(file_path, processed_file_dest)

            elif os.path.exists(os.path.join(processed_file_dest, filename)):
                ii = 1
                while True:
                    new_name = os.path.join(processed_file_dest, filename + "_" + str(ii) + "." + file_extent)
                    if not os.path.exists(new_name):
                        shutil.move(event.src_path, new_name)
                        print("Copied", file_path, "as", new_name)
                        break
                    ii += 1

        else:
            if file_extent != "xlsx":
                print("non excel file has been created")
                if not os.path.exists(os.path.join(no_apply_dest, filename)):
                    print(f"{no_apply_dest}no existe")
                    shutil.move(file_path, no_apply_dest)

                elif os.path.exists(os.path.join(no_apply_dest, filename)):
                    ii = 1
                    while True:
                        new_name = os.path.join(no_apply_dest, filename + "_" + str(ii) + "." + file_extent)
                        if not os.path.exists(new_name):
                            shutil.move(event.src_path, new_name)
                            print("Copied", file_path, "as", new_name)
                            break
                        ii += 1

    def on_deleted(self, event):
        print(f"file deleted at {event.src_path}")


watcher_path = input(f"Enter location path of the folder to monitor\n")
master_xl = input(f"Enter location path of the folder where master.xlsx exists (MUST EXIST)\n")
processed_file_dest = input(f"Enter location path for the folder to place processed files \n")
no_apply_dest = input(f"Enter location path of the folder to store non applicable files \n")

event_handler = Handler()
observer = watchdog.observers.Observer()
observer.schedule(event_handler, watcher_path, recursive=False)
observer.start()
try:
    while True:
        time.sleep(1)
finally:
    observer.stop()
