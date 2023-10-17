
####################################################################################################################
# Input
note_id_field = 'note_id' # fieldname of note_id in json input
note_text_field = 'note_text' # fieldname of note_text in json input

json_l_note = '/home/shenq/tide_orch/sample_notes_jsonl/big_notes.json' # input json file
output_jsonl = 'output/output.json' # will APPEND to this file if already exists, will create this file if not exists


tide_dir = '/home/shenq/tide' # dir of tide
deidConfigFile=f'{tide_dir}/src/main/resources/deid_config_omop_genrep.yaml' # deidConfigFIle

chunksize = 10000 # num of row for each chunk
jvm_memory = '16g' # memory allocated to java virtual machine
#####################################################################################################################


import pandas as pd
import logging
import os
import subprocess
import shutil

# set logger
logging.basicConfig(level=logging.DEBUG)

# generator to read jsonl file in chunks
json_reader = pd.read_json(
    json_l_note, 
    lines=True,
    chunksize=chunksize
)

# create tmp folder
tmp_dir = 'tmp'
os.makedirs(tmp_dir, exist_ok=True)

i = 1
for input_chunk in json_reader:
    start_line = (i-1)*chunksize + 1
    end_line = (i-1)*chunksize + len(input_chunk.index)
    logging.info('*'*50)
    logging.info(f'Start of Chunk {i}: Line# {start_line} - {end_line}'.center(50))
    logging.info('*'*50)

    # Save chunk input file to tmp folder
    input_chunk_tmp_fp = os.path.join(tmp_dir, f'input_notes_chunk_{i}.jsonl')
    logging.info(f'Writing Chunk JSONL Input File to {os.path.abspath(input_chunk_tmp_fp)}')
    input_chunk.to_json(input_chunk_tmp_fp, orient='records', lines=True)
    logging.info(f'Chunk JSONL Input File Saved: input_notes_chunk_{i}.jsonl')

    # Run TiDe
    logging.info(f'----------Running TiDE on Chunk {i}----------')
    logging.info('Start Running TiDE')
    tmp_output_dir = os.path.abspath(os.path.join(tmp_dir, f'output_chunk_{i}'))
    tide_command = f'''java -Xmx{jvm_memory} -jar {tide_dir}/target/deid-3.0.31-SNAPSHOT-dataflow.jar \
  --inputType=local \
  --inputResource="{os.path.abspath(input_chunk_tmp_fp)}" \
  --phiFileName="{os.path.abspath(input_chunk_tmp_fp)}" \
  --personFile="{os.path.abspath(input_chunk_tmp_fp)}" \
  --textIdFields="{note_id_field}" \
  --textInputFields="{note_text_field}" \
  --outputResource="{tmp_output_dir}" \
  --deidConfigFile="{deidConfigFile}" \
  --annotatorConfigFile={tide_dir}/src/main/resources/annotator_config.yaml'''
    logging.debug(f'Running Command: \n{tide_command}')
    with open(f'{tmp_dir}/tide_log_chunk_{i}.log','w') as tide_log:
        shell_result = subprocess.run(
            tide_command,
            stdout=tide_log,
            shell=True
        )
    if shell_result.returncode != 0: #if non-zero return code
        logging.error(shell_result.stderr)
        raise Exception(f'TiDE Failed on Chunck {i}: Line# {start_line} - {end_line}')
    else:
        logging.info('TiDE run successfully')
        #logging.debug(shell_result.stdout)
        #logging.debug(shell_result.stderr)
    
    # Extract and Merge Output 
    logging.info(f'---------Extracting and Merging Chunk {i}---------')
    logging.info(f'Output will be append to {output_jsonl}')
    for filename in os.listdir(tmp_output_dir):
        fp = os.path.join(tmp_output_dir, filename)
        if os.path.isfile(fp) and 'DeidNote-' in fp: # if filename contains DeidNote-
            with open(output_jsonl, mode="a") as output_f:
                logging.info(f'Extracting and Merging {fp}')
                df = pd.read_json(fp, lines=True)[[
                    note_id_field, 
                    f'TEXT_DEID_{note_text_field}']
                ]
                output_f.write(df.to_json(orient="records", lines=True))
    logging.info(f'Extracting and Merging Finished')

    # Delete tmp Files
    logging.info(f'---------Delete tmp Files Chunk {i}---------')
    logging.info(f"Deleting input chunk tmp file: {input_chunk_tmp_fp}")
    os.remove(input_chunk_tmp_fp)
    logging.info(f"Deleting output chunk tmp dir: {tmp_output_dir}")
    shutil.rmtree(tmp_output_dir)

    logging.info(f'Chunk {i} Processed Successfully'.center(50,'-'))
    i += 1


    

