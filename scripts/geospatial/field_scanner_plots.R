#####################generate standard input####################################
ranges=54
columns=16
plot = 1:(ranges*columns)
grid=data.frame(plot)
grid$column=(grid$plot-1)%%columns+1
grid$range=floor((grid$plot-1)/columns)+1

for ( i in (1:(ranges/2))*2 )
{
  grid[((i-1)*columns+1):(i*columns), 1]=c((i*columns):((i-1)*columns+1))
}
grid=grid[order(grid$plot),]

range=read.csv("range.csv")
row=read.csv("row.csv")
column=matrix(0,nrow=columns,ncol=3)
column=as.data.frame(column)
names(column)=c("column","y_west","y_east")
for(i in 1:columns)
{
  column[i,1]=i
  column[i,3]=row[i*2,3]
  column[i,2]=row[i*2-1,2]
}
grid$x_south=rep(0,ranges*columns)
grid$x_north=rep(0,ranges*columns)
grid$y_west=rep(0,ranges*columns)
grid$y_east=rep(0,ranges*columns)
for(i in 1:(ranges*columns))
{
  grid$x_south[i]=range[range$range==grid$range[i],]$x_south
  grid$x_north[i]=range[range$range==grid$range[i],]$x_north
  grid$y_west[i]=column[column$column==grid$column[i],]$y_west
  grid$y_east[i]=column[column$column==grid$column[i],]$y_east
}

write.csv(grid,"input.csv")
#################################################################################

#####################Projection and Transformation###############################
require('proj4')
options(digits=15)

gantry2latlon <- function(Gx,Gy)
{
  
  proj='+proj=utm +zone=12 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'
  
  ay = 3659974.971 ; by = 1.0002 ; cy = 0.0078 ;
  ax = 409012.2032 ; bx = 0.009 ; cx = - 0.9986;
  
  # gantry --> MAC
  Mx = ax + bx * Gx + cx * Gy
  My = ay + by * Gx + cy * Gy
  result=project(cbind(Mx,My),proj,inverse=T)
  
  # MAC --> USDA
  result[,1]=result[,1]+0.000020308287
  result[,2]=result[,2]-0.000015258894
  return(result)
}

latlon2length <- function(lon1,lat1,lon2,lat2)
{
  
  proj='+proj=utm +zone=12 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'
  UTM_pt1=project(cbind(lon1,lat1),proj)
  UTM_pt2=project(cbind(lon2,lat2),proj)
  length=sqrt((UTM_pt1[,1]-UTM_pt2[,1])^2+(UTM_pt1[,2]-UTM_pt2[,2])^2)
  return(length)
}


grid$pt1X=grid$x_south
grid$pt1Y=grid$y_west
grid$pt2X=grid$x_south
grid$pt2Y=grid$y_east
grid$pt3X=grid$x_north
grid$pt3Y=grid$y_east
grid$pt4X=grid$x_north
grid$pt4Y=grid$y_west

grid$plot_W=abs(grid$y_west-grid$y_east)
grid$plot_L=abs(grid$x_north-grid$x_south)

grid$lon1=gantry2latlon(grid$pt1X,grid$pt1Y)[,1]
grid$lat1=gantry2latlon(grid$pt1X,grid$pt1Y)[,2]
grid$lon2=gantry2latlon(grid$pt2X,grid$pt2Y)[,1]
grid$lat2=gantry2latlon(grid$pt2X,grid$pt2Y)[,2]
grid$lon3=gantry2latlon(grid$pt3X,grid$pt3Y)[,1]
grid$lat3=gantry2latlon(grid$pt3X,grid$pt3Y)[,2]
grid$lon4=gantry2latlon(grid$pt4X,grid$pt4Y)[,1]
grid$lat4=gantry2latlon(grid$pt4X,grid$pt4Y)[,2]

grid$UTM_W=latlon2length(grid$lon1,grid$lat1,grid$lon2,grid$lat2)
grid$UTM_L=latlon2length(grid$lon2,grid$lat2,grid$lon3,grid$lat3)

##################################################################################

########################### write SQL ############################################

cat("",file="output_sql.txt",sep="")
for(i in 1:(ranges*columns))
{
#   tablename = "yourtablename"
#   sql = paste("INSERT INTO", tablename, "(plotid,range,col,geomgantry,x,y,gantryw,gantryl,geomusda,lon,lat,utmw,utml)")
#   temp=paste0(grid$pt1X[i]," ",grid$pt1Y[i],", ",
#               grid$pt2X[i]," ",grid$pt2Y[i],", ",
#               grid$pt3X[i]," ",grid$pt3Y[i],", ",
#               grid$pt4X[i]," ",grid$pt4Y[i],", ",
#               grid$pt1X[i]," ",grid$pt1Y[i])
#   geom_gantry=paste0("ST_GeomFromText('POLYGON((",temp,"))'",")")
 
  temp=paste0(grid$lon1[i]," ",grid$lat1[i]," 353, ",
              grid$lon2[i]," ",grid$lat2[i]," 353, ",
              grid$lon3[i]," ",grid$lat3[i]," 353, ",
              grid$lon4[i]," ",grid$lat4[i]," 353, ",
              grid$lon1[i]," ",grid$lat1[i]," 353")
  geom_latlon=paste0("ST_GeomFromText('POLYGON((",temp,"))'",", 4326)")
  
#   sql = paste(sql,"VALUES (",paste(grid$plot[i],grid$range[i], grid$column[i],geom_gantry,grid$pt1X[i],grid$pt1Y[i],
#                                    grid$plot_W[i],grid$plot_L[i],geom_latlon,grid$lon1[i],grid$lat1[i],
#                                    grid$UTM_W[i],grid$UTM_L[i],sep=", "),");")
  
  sql=paste0("INSERT INTO sites (sitename, geometry) VALUES ( ", 
        paste0("'MAC Field Scanner Field Plot ", grid$plot[i], " Season 2', "),
        geom_latlon, ");")
  
  cat(sql,file="output_sql.txt",sep="\n",append=TRUE)
}














