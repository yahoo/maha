/**
 * Created by vivekch on 5/6/16.
 */
google.charts.load('current', {'packages':['table']});
//google.charts.setOnLoadCallback();


function dataSourcePicker() {
    console.log("data source picker")
    $("#Dimensions").empty()
    $("#Metrics").empty()
    $("#mainBtn").empty()
    $("#myPieChart").empty()
    $("#alert").remove()
    $("#result").empty()
    $("#load").hide()
    var datasource = document.getElementById("dataSources").value
    console.log(" selected datasource is "+ datasource )
    dimensionGenerator(datasource)
    metricsGenerator(datasource)

}

function submitButtonGenerator(src){
    g_s=src
    console.log("submit button")
    var subDiv = $("#mainBtn")
    subDiv.append('<button onclick="queryGenerator()" class="btn btn-info col-md-4"> <span class="glyphicon glyphicon-ok-sign"></span> Execute</button>')
}

function queryGenerator(){
    $("#alert").remove()
    $("#result").empty()
    $("#myPieChart").empty()
    var dim = document.getElementById("dims").value
    console.log(dim)
    var value = document.getElementById("val").value
    var start= $("#datetimepickerstart").data().date
    var end= $("#datetimepickerend").data().date
    var startDate = $("#datetimepickerstart").data().date
    var endDate =  $("#datetimepickerend").data().date
    if(value==""){
        $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Filter value missing</strong> </div>')

    }
    else if((end-start)>31){
        $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong>Time interval greater than 31</strong> </div>')
    }
    else{
        console.log(value)
        console.log(start + " " +end)
        getSegments(start,end,dim,value)
    }

}

function getSegments(start,end,dim,value){
    var query='{"queryType": "groupBy","dataSource": {"type": "table","name": \"'+g_s+'\"},"intervals": {"type": "intervals","intervals": [\"'+start+'/'+end+'\"]},"filter": {"type": "and","fields": [{"type": "selector","dimension": \"'+dim+'\","value": \"'+value+'\"}]},"granularity": {"type": "all"},"dimensions": [{"type": "default","dimension": \"'+dim+'\","outputName": \"'+dim+'\"}],"aggregations": [{"type": "count","name": "Count"}],"having": null,"context": {"groupByIsSingleThreaded" : false,"chunkPeriod" : "P3D","populateCache": true,"bySegment": true,"timeout": 300000}}'
    console.log(query)
    var route = jsRoutes.controllers.DashBoard.getSegments
    console.log(" route "+ route)
    var start = new Date().getTime();
    route().ajax({
        data : query,
        contentType : 'application/json',
        beforeSend: function(){
            $('#load').show();
        },
        complete: function(){
            $('#load').hide();
        },
        timeout: 60000,
        success: function(result) {
            try{
                var end  = new Date().getTime();
                var diff =end - start;
                console.log('milliseconds passed', end - start);
                var data =[]
                data.push(['Segments','Count'])
                console.log("query result are : " +result)
                var res = JSON.parse(result)
                res.forEach(function(r){
                    var inData=[]
                    console.log(r.result.segment)
                    inData.push(r.result.segment)
                    if(r.result.results.length >0){
                        console.log(r.result.results[0].event.Count)
                        inData.push(r.result.results[0].event.Count.toString())
                    }else{
                        inData.push("0".toString())
                    }
                    data.push(inData)
                })
                generateChart(data,diff)
            }catch(err){
                $('#load').hide();
                console.log(" failed to execute query"+ err)
                $("#alert").remove()
                $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Query execution failed</strong> </div>')

            }
        },
        error: function(x, t, m) {
            console.log(" failed to execute the query "+ t)
            $("#alert").remove()
            $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Error occured while executing query: '+t+'</strong> </div>')

        }
        ,
        failure: function(x) {
            console.log(" failed to execute the query "+ t)
            $("#alert").remove()
            $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Error occured while getting segments:'+t+'</strong> </div>')

        }
    })
}

function generateChart(d,diff) {
    console.log("generating chart")
    $("#myPieChart").empty()
    $("#result").empty()
    // Define the chart to be drawn.
    try {
        var data = new google.visualization.DataTable();
        data.addColumn('string', 'Segment');
        data.addColumn('string', 'Count')
        data.addRows(d)
        var data = new google.visualization.arrayToDataTable(d);
        var table = new google.visualization.Table(document.getElementById('myPieChart'));
        data.sort({column: 1, desc: true});
        table.draw(data, {showRowNumber: true, width: '100%', height: '100%'});
        $("#result").append('<b>Time taken to execute : '+diff+' ms</b>')
    }
    catch(err){
        console.log(" failed to draw chart "+ err)
        $("#alert").remove()
        $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Data visualization error : Unexpected query result</strong> </div>')

    }
}



function metricsGenerator(source){
    console.log("Getting metrics for the data source  " + source)
    var route = jsRoutes.controllers.DashBoard.getMetrics
    route(source).ajax({
        timeout: 60000,
        beforeSend: function(){
            $('#load').show();
        },
        complete: function(){
            $('#load').hide();
        },
        success: function(result) {
            try{
                var res = JSON.parse(result)
                console.log("metrics are : " +res)
                metricsBuilder("Metrics",res)
            }catch(err){
                console.log("Metrics generation failed "+ err)
                $("#alert").remove()
                $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong>  Error occured while getting metrics:'+err+'</strong> </div>')

            }

        },
        error: function(x, t, m) {
            console.log(" failed to fetch the metrics "+ t)
            $("#alert").remove()
            $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Error occured while fetching metrics : '+t+'</strong> </div>')

        },
        failure: function(err) {
            console.log(" failed to get the metrics "+ err)
            $("#alert").remove()
            $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Error occured while getting metrics :'+err+'</strong> </div>')

        }
    })

}



function metricsBuilder(id,result){
    var parent = document.getElementById(id)
    var elements = $();
    var row1 = $('<div class ="row">')
    var label = row1.append('<label class="text-primary "> <h5>' +id+'  : '+ result.length+'</h5></label>')
    elements=elements.add(row1)
    elements=elements.add('<br/>')
    result.forEach(function(res){
        var mainDiv=$('<div class="row">')
        var labelDiv=$('<div class="col-mid-12  text-center">')
        labelDiv=labelDiv.append('<label type="text" class="text-primary" for=\"'+res+'\">'+res+'<div>')
        mainDiv=mainDiv.append(labelDiv)
        elements=elements.append(mainDiv)
    })
    $(parent).append(elements)
}


function dimBuilder(id,result,source){
    var parent = document.getElementById(id)
    var elements = $();
    var row1 = $('<div class ="row">')
    var label = row1.append('<label class="text-primary "> <h5>' +id+'  : '+ result.length+'</h5></label>')
    elements=elements.add(row1)
    elements=elements.add('<br/>')
    var mainDiv=$('<div class="row">')
    var descColumn = $('<div class="col-md-3 text-primary"> <b>Filter</b></div>')
    mainDiv = mainDiv.append(descColumn)
    var selectColumn = $('<div class="col-md-3">')
    var options = $('<select id="dims" name="dims">')
    result.forEach(function(res){
        options = options.append('<option class=" text-primary" value=\"'+res+'\">'+res+'</option>')
    })
    selectColumn=selectColumn.append(options)
    mainDiv=mainDiv.append(selectColumn)
    mainDiv=mainDiv.append('<div class="col-md-3 text-center text-primary"><b>Value</b> </div>')
    var inputDiv = $('<div class="col-xs-3 form-group">')
    inputDiv = inputDiv.append('<input class="form-control" type="text" id="val">')
    mainDiv=mainDiv.append(inputDiv)
    elements=elements.append(mainDiv)
    $(parent).append(elements)
    if(result==""||result.length==0||result==undefined||result==NaN){
        $("#alert").remove()
        console.log("no dimension are present")
        $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong>Cannot query the data source ... no dimension present</strong> </div>')

    }else {
        submitButtonGenerator(source)
    }
}

function dimensionGenerator(source){
    console.log("Getting dimensions for the data source " + source)
    var route = jsRoutes.controllers.DashBoard.getDimensions
    route(source).ajax({
        beforeSend: function(){
            $('#load').show();
        },
        complete: function(){
            $('#load').hide();
        },
        timeout: 60000,
        error: function(x, t, m) {
            console.log(" failed to get dimension "+ t)
            $("#alert").remove()
            $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Error occured while fetching dimensions : '+t+'</strong> </div>')

        },
        success: function(result) {
            try {
                var res = JSON.parse(result)
                dimBuilder("Dimensions", res, source)
            }catch(err){
                console.log("Dimension generation failed "+ err)
                $("#alert").remove()
                $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong>  Error occured while getting dimension:'+err+'</strong> </div>')

            }
        },
        failure: function(err) {
            console.log(" failed to get the dimensions "+err)
            $("#alert").remove()
            $("#container").append('<div class="alert alert-danger fade in" id="alert"> <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a> <strong> Error occured while getting dimension:'+err+'</strong> </div>')
        }
    })
}
