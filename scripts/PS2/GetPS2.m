
[FileName,PathName,FilterIndex] = uigetfile('*.bin');
pkg image load
clear M
close all

D=dir(PathName);



for i=4:size(D,1)-1


fileID = fopen([PathName D(i).name]);
A = fread(fileID,[1936,1216],'uint8');
A=double(A)./255;

M(i-3)=mean(mean(A));



end

figure(1),subplot(3,1,1),plot(M), xlabel("frame"),ylabel("mean intensity")

% get Frame 1 as Fdark
fileID = fopen([PathName D(4).name]);
Fdark = fread(fileID,[1936,1216],'uint8');
Fdark=double(Fdark)./255;
% get Frame 2 as Fv
fileID = fopen([PathName D(5).name]);
F0 = fread(fileID,[1936,1216],'uint8');
% subtract Fdark
F0=double(F0)./255-Fdark;

fileID = fopen([PathName D(40).name]);
Fm = fread(fileID,[1936,1216],'uint8');
% subtract Fdark
Fm=double(Fm)./255-Fdark;
FmHist=reshape(Fm,1,1936*1216);

% image mask
threshold=max(max(Fm))/10;

mask=Fm>threshold & (Fm-F0)>=0;

% clean up a bit
se = strel ("square", 5);
% remove background
mask=imerode (mask, se);
% fill holes
mask=imdilate(mask, se);

FvFm=(Fm-F0)./Fm.*mask;

 colormap('hot')
figure(1),subplot(3,1,2),imagesc(FvFm),colorbar, axis off

FvFm(FvFm==0)=NaN;
FvFmHist=reshape(FvFm,1,1936*1216);
figure(1),subplot(3,1,3),hist(FvFmHist,50),, xlim([0,1]),xlabel("Fv/Fm"),ylabel("no of pixel")

