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
## @deftypefn {Function File} {@var{retval} =} PlotNDVI (@var{input1}, @var{input2})
##
## @seealso{}
## @end deftypefn

## Author: LemnaTec <LemnaTec@S1_CONTAINER>
## Created: 2016-06-04

function [retval] = PlotNDVI (input1, input2)

[PathName] = uigetdir();

D=dir(PathName);

for i=3:size(D,1)

[NDVI(i-2),x(i-2),y(i-2),z(i-2),t(i-2)]=GetNDVI([PathName '\' D(i).name]);

end

colorMap=jet(length(unique(NDVI)));
set(gcf, 'ColorMap', colorMap);
h=plot(x,y);

endfunction
