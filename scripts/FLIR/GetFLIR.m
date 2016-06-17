[FileName,PathName,FilterIndex] = uigetfile('*.bin');

fileID = fopen([PathName FileName]);
A = fread(fileID,[640,480],'uint16');


%rescale to visible range
Gmin=2800;
Gmax=3300;

At=((A-Gmin)/(Gmax-Gmin));

 colormap('hot')
figure(1),imagesc(At),colorbar, axis off