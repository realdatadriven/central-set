import{S as Pe,i as Se,s as je,v as qe,I as ee,e as u,a as w,t as k,c as te,b as _,a0 as Le,d as ie,f as n,a1 as De,m as le,l as K,n as j,g as ae,j as ne,o as _e,x as Me,p as se,D as Ne,q as ze,r as Ae,E as Be,u as Fe,w as Ge,$ as He,C as oe}from"./index-8239faad.js";function Ee(e,l,i){const r=e.slice();return r[13]=l[i],r}function Je(e){let l,i=e[13]+"",r,s;return{c(){l=u("option"),r=k(i),l.__value=s=e[13],oe(l,l.__value)},m(a,o){ie(a,l,o),n(l,r)},p(a,o){o&2&&i!==(i=a[13]+"")&&j(r,i),o&2&&s!==(s=a[13])&&(l.__value=s,oe(l,l.__value))},d(a){a&&_e(l)}}}function Ke(e){let l,i=e[4].t(`crud.${e[13].label}`)+"",r,s;return{c(){l=u("option"),r=k(i),l.__value=s=e[13].value,oe(l,l.__value)},m(a,o){ie(a,l,o),n(l,r)},p(a,o){o&18&&i!==(i=a[4].t(`crud.${a[13].label}`)+"")&&j(r,i),o&2&&s!==(s=a[13].value)&&(l.__value=s,oe(l,l.__value))},d(a){a&&_e(l)}}}function Ie(e){let l;function i(a,o){return a[13].value?Ke:Je}let r=i(e),s=r(e);return{c(){s.c(),l=Ge()},m(a,o){s.m(a,o),ie(a,l,o)},p(a,o){r===(r=i(a))&&s?s.p(a,o):(s.d(1),s=r(a),s&&(s.c(),s.m(l.parentNode,l)))},d(a){a&&_e(l),s.d(a)}}}function Oe(e){var ke;let l,i,r,s,a,o,C,q=e[4].t("crud.rows_per_page")+"",v,O,Q,g,R,c,L=(e[0]===-1?1:e[2]*e[0]+1)+"",d,de,T=(e[0]===-1?e[3]:e[2]*e[0]+e[0])+"",re,be,U=e[4].t("crud.of")+"",ue,ge,pe,ce,m,D,M,N,V,W,he,E,z,A,X,Y,ve,I,B,F,Z,y,me,P,G,H,$,x,f,fe,we,J=qe(((ke=e[1])==null?void 0:ke.rows_per_pag_opts)||[5,10,15,20,25,50,100,500,{label:"all",value:-1}]),b=[];for(let t=0;t<J.length;t+=1)b[t]=Ie(Ee(e,J,t));return N=new ee({props:{type:"outline",icon:"chevron-double-left",path:"icons"}}),A=new ee({props:{type:"outline",icon:"chevron-left",path:"icons"}}),F=new ee({props:{type:"outline",icon:"chevron-right",path:"icons"}}),H=new ee({props:{type:"outline",icon:"chevron-double-right",path:"icons"}}),{c(){l=u("div"),i=u("div"),r=w(),s=u("div"),a=u("div"),o=u("label"),C=u("span"),v=k(q),O=k(":"),Q=w(),g=u("select");for(let t=0;t<b.length;t+=1)b[t].c();R=w(),c=u("div"),d=k(L),de=k(" - "),re=k(T),be=w(),ue=k(U),ge=w(),pe=k(e[3]),ce=w(),m=u("div"),D=u("button"),M=u("span"),te(N.$$.fragment),he=w(),E=u("button"),z=u("span"),te(A.$$.fragment),ve=w(),I=u("button"),B=u("span"),te(F.$$.fragment),me=w(),P=u("button"),G=u("span"),te(H.$$.fragment),_(i,"class","grow"),_(C,"class","label-text pr-2"),_(g,"class","select w-30 select-sm"),e[0]===void 0&&Le(()=>e[7].call(g)),_(o,"class","label cursor-pointer pt-0 m-0 pb-0"),_(a,"class","form-control"),_(s,"class","h-8 pt-0 pr-2"),_(c,"class","h-8 pt-0.5 pr-2"),_(M,"class","w-6 tooltip tooltip-left normal-case"),_(M,"data-tip",V=e[4].t("crud.first")),_(D,"class","btn btn-square btn-ghost btn-sm"),D.disabled=W=e[2]*e[0]+e[0]<2*e[0]||e[0]===-1,_(z,"class","w-6 tooltip tooltip-left normal-case"),_(z,"data-tip",X=e[4].t("crud.back")),_(E,"class","btn btn-square btn-ghost btn-sm"),E.disabled=Y=e[2]*e[0]+e[0]===e[0]||e[0]===-1,_(B,"class","w-6 tooltip tooltip-left normal-case"),_(B,"data-tip",Z=e[4].t("crud.next")),_(I,"class","btn btn-square btn-ghost btn-sm"),I.disabled=y=e[2]*e[0]+e[0]>=e[3]||e[0]===-1,_(G,"class","w-6 tooltip tooltip-left normal-case"),_(G,"data-tip",$=e[4].t("crud.last")),_(P,"class","btn btn-square btn-ghost btn-sm"),P.disabled=x=e[2]*e[0]+e[0]>=e[3]||e[0]===-1,_(m,"class","btn-group"),_(l,"class","flex flex-row overflow-hidden p-2")},m(t,p){ie(t,l,p),n(l,i),n(l,r),n(l,s),n(s,a),n(a,o),n(o,C),n(C,v),n(C,O),n(o,Q),n(o,g);for(let S=0;S<b.length;S+=1)b[S]&&b[S].m(g,null);De(g,e[0],!0),n(l,R),n(l,c),n(c,d),n(c,de),n(c,re),n(c,be),n(c,ue),n(c,ge),n(c,pe),n(l,ce),n(l,m),n(m,D),n(D,M),le(N,M,null),n(m,he),n(m,E),n(E,z),le(A,z,null),n(m,ve),n(m,I),n(I,B),le(F,B,null),n(m,me),n(m,P),n(P,G),le(H,G,null),f=!0,fe||(we=[K(g,"change",e[7]),K(g,"change",e[8]),K(D,"click",e[9]),K(E,"click",e[10]),K(I,"click",e[11]),K(P,"click",e[12])],fe=!0)},p(t,[p]){var S;if((!f||p&16)&&q!==(q=t[4].t("crud.rows_per_page")+"")&&j(v,q),p&18){J=qe(((S=t[1])==null?void 0:S.rows_per_pag_opts)||[5,10,15,20,25,50,100,500,{label:"all",value:-1}]);let h;for(h=0;h<J.length;h+=1){const Ce=Ee(t,J,h);b[h]?b[h].p(Ce,p):(b[h]=Ie(Ce),b[h].c(),b[h].m(g,null))}for(;h<b.length;h+=1)b[h].d(1);b.length=J.length}p&3&&De(g,t[0]),(!f||p&5)&&L!==(L=(t[0]===-1?1:t[2]*t[0]+1)+"")&&j(d,L),(!f||p&13)&&T!==(T=(t[0]===-1?t[3]:t[2]*t[0]+t[0])+"")&&j(re,T),(!f||p&16)&&U!==(U=t[4].t("crud.of")+"")&&j(ue,U),(!f||p&8)&&j(pe,t[3]),(!f||p&16&&V!==(V=t[4].t("crud.first")))&&_(M,"data-tip",V),(!f||p&7&&W!==(W=t[2]*t[0]+t[0]<2*t[0]||t[0]===-1))&&(D.disabled=W),(!f||p&16&&X!==(X=t[4].t("crud.back")))&&_(z,"data-tip",X),(!f||p&7&&Y!==(Y=t[2]*t[0]+t[0]===t[0]||t[0]===-1))&&(E.disabled=Y),(!f||p&16&&Z!==(Z=t[4].t("crud.next")))&&_(B,"data-tip",Z),(!f||p&15&&y!==(y=t[2]*t[0]+t[0]>=t[3]||t[0]===-1))&&(I.disabled=y),(!f||p&16&&$!==($=t[4].t("crud.last")))&&_(G,"data-tip",$),(!f||p&15&&x!==(x=t[2]*t[0]+t[0]>=t[3]||t[0]===-1))&&(P.disabled=x)},i(t){f||(ae(N.$$.fragment,t),ae(A.$$.fragment,t),ae(F.$$.fragment,t),ae(H.$$.fragment,t),f=!0)},o(t){ne(N.$$.fragment,t),ne(A.$$.fragment,t),ne(F.$$.fragment,t),ne(H.$$.fragment,t),f=!1},d(t){t&&_e(l),Me(b,t),se(N),se(A),se(F),se(H),fe=!1,Ne(we)}}}function Qe(e,l,i){let r;ze(e,Ae,d=>i(4,r=d));let{lang:s="en"}=l,{conf:a={}}=l,{rows_per_page:o=10}=l,{page:C=0}=l,{total_rows:q=0}=l;const v=Be();Fe(()=>{r.changeLanguage(s)});function O(){o=He(this),i(0,o),i(1,a)}const Q=()=>v("pageChange",{type:"rows_per_page",rows_per_page:o}),g=()=>v("pageChange",{type:"first"}),R=()=>v("pageChange",{type:"back"}),c=()=>v("pageChange",{type:"next"}),L=()=>v("pageChange",{type:"last"});return e.$$set=d=>{"lang"in d&&i(6,s=d.lang),"conf"in d&&i(1,a=d.conf),"rows_per_page"in d&&i(0,o=d.rows_per_page),"page"in d&&i(2,C=d.page),"total_rows"in d&&i(3,q=d.total_rows)},[o,a,C,q,r,v,s,O,Q,g,R,c,L]}class Te extends Pe{constructor(l){super(),Se(this,l,Qe,Oe,je,{lang:6,conf:1,rows_per_page:0,page:2,total_rows:3})}}export{Te as P};
