## Copyright (C) 2016 LemnaTec
## 
## This program is free software; you can redistribute it and/or modify it
## under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.

## -*- texinfo -*- 
## @deftypefn {Function File} {@var{retval} =} GetNDVI (@var{input1}, @var{input2})
##
## @seealso{}
## @end deftypefn

## Author: LemnaTec <LemnaTec@S1_CONTAINER>
## Created: 2016-06-03

function [NDVI,x,y,z,time] = GetNDVI (PathName)

if nargin==0

[PathName] = uigetdir();

end


D=dir(PathName);

% read NDVI from file
Text=importdata([PathName '\' D(4).name]);
fi=findstr(Text{2},'"');
NDVI=str2num(Text{2}(fi(3)+1:fi(4)-1));

% read x,y,z from file
Text=importdata([PathName '\' D(3).name]);
% x = row 22 y = row 23 z = row 24
fi=findstr(Text{22},'"');
x=str2num(Text{22}(fi(3)+1:fi(4)-1));

fi=findstr(Text{23},'"');
y=str2num(Text{23}(fi(3)+1:fi(4)-1));

fi=findstr(Text{24},'"');
z=str2num(Text{24}(fi(3)+1:fi(4)-1));

% read time
Text=importdata([PathName '\' D(3).name]);
fi=findstr(Text{21},'"');
timestr=Text{21}(fi(3)+1:fi(4)-1);
Year=timestr(7:10);
Month=timestr(1:2);
Day=timestr(4:5);
Hour=timestr(12:13);
Minute=timestr(15:16);

time = datenum(str2num(Year), str2num(Month), str2num(Day), str2num(Hour), str2num(Minute));

endfunction
