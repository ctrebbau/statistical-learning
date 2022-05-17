module Wrangle

using CSV, DataFrames, Dates, Statistics, PyCall, MLJ

function wrangle(path="/Users/CAT79/Job/sql_scripts/RequestArrivalTime/request_pick.csv")
	function distance_between_coordinates(lat1, lon1, lat2, lon2) 
		earth_radius = 6371
		degrees_to_radians(degrees) = degrees * π / 180
	
		lat1 = degrees_to_radians(lat1)
		lat2 = degrees_to_radians(lat2)
	
		dLat = degrees_to_radians(lat2-lat1)
		dLon = degrees_to_radians(lon2-lon1)
	  
		
	  
		a = sin(dLat/2) * sin(dLat/2) +
			sin(dLon/2) * sin(dLon/2) * cos(lat1) * cos(lat2) 
		
		c = 2 * atan(sqrt(a), sqrt(1-a)) 
		return earth_radius * c
	end

    es_holidays = pyimport("holidays").ES(years=[2018, 2019, 2020, 2021, 2022])

	df = CSV.File(path, dateformat = "y-m-d H:M:S") |> DataFrame
	
	df[!, :RequestHr]  = Dates.hour.(df[!,   :RequestServiceTime])
	df[!, :RequestMin] = Dates.minute.(df[!, :RequestServiceTime])
	df[!, :RequestSec] = Dates.second.(df[!, :RequestServiceTime])
	
	df[!, :StartServiceHr] = Dates.hour.(df[!, :StartServiceTime])
	df[!, :StartServiceMin] = Dates.minute.(df[!, :StartServiceTime])
	df[!, :StartServiceSec] = Dates.second.(df[!, :StartServiceTime])

    # Edge case: StartService != StartService
    df[!, :WeekDay] = Dates.dayofweek.(df[!, :RequestServiceTime])
    df[!, :Holiday] = map(d -> Date(d) in keys(es_holidays) ? es_holidays[Date(d)] : "workday", 
                               df[!, :StartServiceTime])
	
    df[!, :TimeDiff] = df[!, :StartServiceTime] .- df[!, :RequestServiceTime]
	df[!, :TimeDiff] = Dates.value.(df[!, :TimeDiff]) ./ 1000

	df[!, :Distance] = distance_between_coordinates.(df[!,:ClientLat], df[!,:ClientLong], df[!,:DriverLat], df[!,:DriverLong]) 

	# Filter Outliers!
    df = filter(df -> df.Distance .>= 0, df)
	df = filter(df -> df.TimeDiff .> 0, df)
	df = df[df[:, :TimeDiff] .< quantile(df[:, :TimeDiff], 0.9), :]
	
	return select!(df, 
		[:RequestServiceTime, :RequestHr, :RequestMin, :RequestSec, :ClientLat, :ClientLong, 
		:DriverLat, :DriverLong, :StartServiceTime, :StartServiceHr, :StartServiceMin, :StartServiceSec,
        :Holiday, :Distance, :TimeDiff])
end

function onehot_holiday()
	OneHotEncoder = @load OneHotEncoder pkg=MLJModels
	df = coerce(df, :Holiday => Multiclass)

	hot = OneHotEncoder(drop_last=false, ordered_factor=false, features=[:Holiday])
	mach = fit!(machine(hot, df))
	df = MLJ.transform(mach, df)
end

function interptime(t)
    hours = t * 2.777778e-4
	minutes = t ÷ 60
	return "$hours hours or $minutes minutes"
end

export wrangle, onehot_holiday, interptime
end # module