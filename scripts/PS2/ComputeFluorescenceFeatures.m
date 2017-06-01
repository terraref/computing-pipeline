% This script computes all characterisitc fluorescence features for
% dark and light-adapted plants

% Variables used here
% F0_dark        - 1936-by-1216          double - F0 Zero fluorescence level for plants just after the excitation light pulse is applied
% Fm_dark        - 1936-by-1216          double - Fm Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv_dark        - 1936-by-1216          double - Fv Fm_dark - F0_dark 
% Fmask_dark     - 1936-by-1216          bool  - Mask to exclude background
% Fm_dark_frame  -                       int8   - frame where Fm_dark is found 

% F0_light       - 1936-by-1216         double - F0' Zero fluorescence level for plants just after the excitation light pulse is applied
% Fm_light       - 1936-by-1216         double - Fm' Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv_light       - 1936-by-1216         double - Fv' Fm_dark - F0_dark 
% Ft_light       - 1936-by-1216         double - Ft  steady-state flourescence in the light 
% Fmask_dark     - 1936-by-1216         bool   - Mask to exclude background
% Fm_light_frame -                      int8   - frame where Fm_dark is found 

% computed values
% FvFm_dark      -  1936-by-1216        double  Fv_dark/Fm_dark The maximal photochemical effiency of PSII
% FvFm_light     -  1936-by-1216        double  Fv_dark/Fm_dark The maximal photochemical effiency of PSII
% Phi_PSII       -  1936-by-1216        double  Quantum yield of photosynthesis
% NPQ            -  1936-by-1216        double  Non-photochemical quenching, absorbed light energy that is dissipated (mostly by thermal radiation)
% qN             -  1936-by-1216        double  Proportion of closed PSII reaction centers
% qP             -  1936-by-1216        double  Proportion of open PSII reaction centers
% Rfd            -  1936-by-1216        double  ratio of chlorophyll decrease to steady state Chlorophyll

pkg image load
clear all
close all




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% load dark adapted PSII data %%%%%%%%%%%%%%%
msgbox("select Filter with Darkadapted plants");
[PathName_dark] = uigetdir;

D=dir(PathName_dark);

% read all frames to compute mean intensity per frame
for i=1:size(D,1)-1 % frame 101 is metadata

  if ~isempty(findstr(D(i).name,'bin'))
    % read frames
    fileID = fopen([PathName_dark '\' D(i).name]);
    A = fread(fileID,[1936,1216],'uint8');
    A=double(A)./255;
    fclose(fileID);
    % Mean intensity
    M(i)=mean(mean(A));
    % FrameIndex from Filename
    FrameIndex(i)=str2num(D(i).name(end-7:end-4));
  end
end


% Fbase = intensity of first frame (without red flash) as base line to subtract
Fbase_i=find(FrameIndex==1);
fileID = fopen([PathName_dark '\' D(Fbase_i).name]);
F_base = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
F_base = double(F_base)./255; % convert to double

% chose frame for Fmax as second highest max value to avoid outlier
[M_sort,SortID]=sort(M);
Fm_i=SortID(end-1);

fileID = fopen([PathName_dark '\' D(Fm_i).name]);
% Fm subtracted by F_base
Fm_dark = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
Fm_dark = double(Fm_dark)./255-F_base; % convert to double

Fm_dark_frame = FrameIndex(Fm_i);

% F0
F0_i=find(FrameIndex==2);
fileID = fopen([PathName_dark '\' D(F0_i).name]);
F0_dark = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
F0_dark = double(F0_dark)./255-F_base; % convert to double

% Compute mask from Fm Frame to exclude background
FmHist=reshape(Fm_dark,1,1936*1216);

% take 99%tile as max intensity as max value
Fsort=sort(FmHist);
Fmax=Fsort(int32(1936*1216*0.99));

% set threshold to 10% of found max value
Fmask_dark=Fm_dark>0.1*Fmax;
%figure(2), hold on, plot([Fsort(int32(1936*1216*0.99)) Fsort(int32(1936*1216*0.99))],[1 50000],'-r')



%%%%%%%% Fv_dark    %%%%%%%
Fv_dark = (Fm_dark - F0_dark).*Fmask_dark;


%%%%%%%% FvFm_dark  %%%%%%%
FvFm_dark = (Fv_dark./Fm_dark).*Fmask_dark;
FvFm_dark(isnan(FvFm_dark))=0;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% load light adapted PSII data %%%%%%%%%%%%%%%
clear Pathname FrameIndex M

msgbox("select Filter with light adapted plants");
[PathName_light] = uigetdir;

D=dir(PathName_light);

% read all frames to compute mean intensity per frame
for i=1:size(D,1)-1 % frame 101 is metadata

  if ~isempty(findstr(D(i).name,'bin'))
    % read frames
    fileID = fopen([PathName_light '\' D(i).name]);
    A = fread(fileID,[1936,1216],'uint8');
    fclose(fileID);
    A=double(A)./255;
    
    % Mean intensity
    M(i)=mean(mean(A));
    % FrameIndex from Filename
    FrameIndex(i)=str2num(D(i).name(end-7:end-4));

  end
end


% Fbase = intensity of first frame (without red flash) as base line to subtract
Fbase_i=find(FrameIndex==1);
fileID = fopen([PathName_light '\' D(Fbase_i).name]);
F_base = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
F_base = double(F_base)./255; % convert to double

% under light condition we asume the flurescence at frame 1 (without red flash) as Ft
Ft_light = F_base;

% chose frame for Fmax as second highest max value to avoid outlier
[M_sort,SortID]=sort(M);
Fm_i=SortID(end-1);

fileID = fopen([PathName_light '\' D(Fm_i).name]);
% Fm subtracted by F_base
Fm_light = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
Fm_light = double(Fm_light)./255-F_base; % convert to double

Fm_light_frame = FrameIndex(Fm_i);

% F0
F0_i=find(FrameIndex==2);
fileID = fopen([PathName_light '\' D(F0_i).name]);
F0_light = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
F0_light = double(F0_light)./255-F_base; % convert to double

% Compute mask from Fm Frame to exclude background
FmHist=reshape(Fm_light,1,1936*1216);

% take 99%tile as max intensity as max value
Fsort=sort(FmHist);
Fmax=Fsort(int32(1936*1216*0.99));

% set threshold to 10% of found max value
Fmask_light=Fm_light>0.1*Fmax;
%figure(2), hold on, plot([Fsort(int32(1936*1216*0.99)) Fsort(int32(1936*1216*0.99))],[1 50000],'-r')



%%%%%%%% Fv_light    %%%%%%%
Fv_light = (Fm_light - F0_light).*Fmask_light;


%%%%%%%% FvFm_light  %%%%%%%
FvFm_light = (Fv_light./Fm_light).*Fmask_light;
FvFm_light(isnan(FvFm_light))=0;
FvFm_light(FvFm_light<0)=0;
 
 
 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%%%%%%%%%%% Compute values dependend on dark and light measurements %%%%%%%%%%%



%%%%% Phi_PSII %%%%%%
Phi_PSII = (Fm_light-Ft_light)./Fm_light.*Fmask_light;

%%%%% NPQ  %%%%%%
NPQ = (Fm_dark-Fm_light)./Fm_light.*Fmask_light;

%%%%% qN %%%%%%%
qN=(Fm_dark-Fm_light)./(Fm_dark-F0_dark).*Fmask_light;

%%%%% qP %%%%%%%
qP=(Fm_light-Ft_light)./(Fm_dark-F0_dark).*Fmask_light;

%%%%% rfd %%%%%%%
Rfd= (Fm_dark./Fm_light-1).*Fmask_light;


PlotFluorescenceFeatures(Fm_dark,Fv_dark,FvFm_dark,Fm_light,Fv_light,FvFm_light,Phi_PSII,NPQ,qN,qP,Rfd)



