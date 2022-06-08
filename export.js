var cloudinary = require('cloudinary').v2
const csv = require('csvtojson')
const converter = require('json-2-csv');
const fs = require('fs');

// Cloudinary configuration
//https://cloudinary.com/documentation/node_integration#setting_configuration_parameters_globally
cloudinary.config({
  cloud_name: '',
  api_key: '',
  api_secret: ''
})

//CSV file containing Structured Metadata to be exported.
const SMD_List_File = 'SMD.csv'

export_dam();

async function export_dam(){
  // read SMD list from SMD_List_File
  const smd = await csv({noheader:true,trim:true}).fromFile(SMD_List_File);
  var smd_arr = [];
  smd.forEach(s => {
      smd_arr.push(s.field1)
  })
  // call function to retrieve the required SMD and all-values for the type single and multi-select dropdowns
  var smd_dictionary = await get_metadata(smd_arr);
  var external_ids=[];
  Object.keys(smd_dictionary).forEach(key => {
      external_ids.push(key)
  })
  let response, next_cursor;
  var result = [];
  var row = {};
  var counter=0;
  console.log('report generation in progress')
  do{
    try{
        response = next_cursor?
            await cloudinary.search.expression('resource_type:image AND type:upload').with_field('metadata').with_field('tags').with_field('context').max_results(500).sort_by('public_id','asc').next_cursor(next_cursor).execute():
            await cloudinary.search.expression('resource_type:image AND type:upload').with_field('metadata').with_field('tags').with_field('context').max_results(500).sort_by('public_id','asc').execute()
    }catch(e){console.error(e)}
    response.resources.forEach(resource => {
      counter++;
      var row = {};
      row['public_id']=resource["public_id"];
      if(resource["etag"])
        row['etag']=resource["etag"].slice(1,-1);
      else 
        row["etag"]="";
      if(resource["tags"])    
        row['tags']=resource["tags"].join("|");
      else
        row['tags']=""
      // collect SMD information  
      row['metadata']=""
      if(resource["metadata"]){
        external_ids.forEach(ext_id => {
          if(resource["metadata"][ext_id]){
            if(smd_dictionary[ext_id]['type']==="enum"){
              row['metadata']=row['metadata']+smd_dictionary[ext_id]['label']+":"+smd_dictionary[ext_id]['datasource'][resource["metadata"][ext_id]]+"|";
            } else
            if(smd_dictionary[ext_id]['type']==="set"){
              // if type=set, then it is of the type Multi-Select. 
              row['metadata']=row['metadata']+smd_dictionary[ext_id]['label']+":";
              resource["metadata"][ext_id].forEach(md => {
                row['metadata']=row['metadata']+smd_dictionary[ext_id]['datasource'][md]+",";
              })
              row['metadata']=row['metadata'].slice(0, -1)+"|"
            } else {
              row['metadata']=row['metadata']+smd_dictionary[ext_id]['label']+":"+resource["metadata"][ext_id];
            }
          }
        })     
      }
      // collect Contextual Metadata Information
      row['alt']="";
      row['title']="";
      if(resource["context"]){
        if(resource["context"]["caption"])
          row['title'] = resource["context"]["caption"]; 
        if(resource["context"]["alt"])
          row['alt'] = resource["context"]["alt"];
      }
      result.push(row)
    })
    next_cursor = response ? response.next_cursor : null
    if(counter%5000==0)
      console.log("In Progress.. completed first "+counter+" assets")
  }while(next_cursor)
  // write CSV to a file
  converter.json2csv(result, (err, csv) => {
    if (err) {
        throw err;
    }
    let today = new Date().toISOString().slice(0, 10)
    fs.writeFileSync(today+'.csv', csv);
    console.log("Report saved in "+today+'.csv')
  });
}

async function get_metadata(smd_arr){
  try{
    // Get a list of all the SMDs present in the cloud
    // https://cloudinary.com/documentation/metadata_api#get_metadata_fields
    var result = await cloudinary.api.list_metadata_fields();
    var smd_dictionary = {};
    result['metadata_fields'].forEach(smd => {
      // check if the SMD is of the interest based on SMD_List_File
      if(smd_arr.includes(smd.label)){
        smd_dictionary[smd.external_id]={};
        smd_dictionary[smd.external_id]['label']=smd.label;
        smd_dictionary[smd.external_id]['type']=smd.type;
        if(smd.type==='enum' || smd.type==='set'){
          // for SMD of the type Single-Select or Mult-Select list, create a mapping between 'External_ID' and 'Value'
          smd_dictionary[smd.external_id]['datasource']={};
          smd.datasource.values.forEach(data => {
              smd_dictionary[smd.external_id]['datasource'][data['external_id']]=data.value;
          })
        }
        else{
          // for SMD NOT of the type Single-Select or Mult-Select list, then there is no datasource
          smd_dictionary[smd.external_id]['datasource']={};  
        }
      }
    })
  }catch(e){
    console.log(e)
  }
  // return the directory created back to the export_dam() function
  return smd_dictionary
}
