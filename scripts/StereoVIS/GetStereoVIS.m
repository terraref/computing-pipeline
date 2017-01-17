close all, clear all

pkg load signal

[FileName,PathName,FilterIndex] = uigetfile('*.bin');

D=dir(PathName);

% read left image
fileID = fopen([PathName D(3).name]);

Aleft = fread(fileID,[3296,2472],'uint8');
Aleft=demosaic(uint8(Aleft),'gbrg')./255;

% use only half resolution due to memory constraint
Aleft = Aleft(1:2:end,1:2:end,:);

% read right image
fileID = fopen([PathName D(5).name]);
Aright = fread(fileID,[3296,2472],'uint8');
Aright=double(demosaic(uint8(Aright),'gbrg'))./255;
Aright = Aright(1:2:end,1:2:end,:);
% combine both images

% add both images
for i=1:3
  Atotal(:,:,i)=[Aleft(:,:,i)' Aright(:,:,i)'];
end


figure(1), imshow(Atotal)

% align both images% add both images
Aalign=[];
cutleft=250;
cutright=980;
for i=1:3
  Aalign(:,:,i)=[Aleft(1:end-cutleft,:,i)' Aright(cutright:end,:,i)'];
end

figure(2), imshow(Aalign);
